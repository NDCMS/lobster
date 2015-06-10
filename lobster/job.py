import multiprocessing
import re
import os
import datetime
import itertools
import shutil
import glob
import gzip
import sqlite3
import subprocess
import time

from functools import partial
from hashlib import sha1
from lobster import chirp, fs, util

logger = multiprocessing.get_logger()

def apply_matching(config):
    defaults = config.get('task defaults', {})
    matching = defaults.get('matching', [])
    configs = []

    for cfg in config['tasks']:
        label = cfg['label']

        for match in matching:
            if re.search(match['label'], label):
                for k, v in match.items():
                    if k == 'label':
                        continue
                    if k not in cfg:
                        cfg[k] = v
        for k, v in defaults.items():
            if k == 'matching':
                continue
            if k not in cfg:
                cfg[k] = v

        configs.append(cfg)

    config['tasks'] = configs

    return config

class JobProvider(object):
    def __init__(self, config):
        self.config = config
        self.basedirs = [config['configdir'], config['startdir']]
        self.workdir = config.get('workdir', os.getcwd())
        self.stageout = config.get('stageout location', os.getcwd())
        self.statusfile = os.path.join(self.workdir, 'status.yaml')

        self.parrot_path = os.path.dirname(util.which('parrot_run'))
        self.parrot_bin = os.path.join(self.workdir, 'bin')
        self.parrot_lib = os.path.join(self.workdir, 'lib')

        self.extra_inputs = {}
        self.args = {}
        self.outputs = {}
        self.outputformats = {}
        self.cmds = {}
        self.bad_exitcodes = config.get('bad exit codes', [])

        chirp_server = config.get('chirp server')
        chirp_root = config.get('chirp root')

        create = not util.checkpoint(self.workdir, 'id') and not self.config.get('merge', False)
        if create:
            self.taskid = 'lobster_{0}_{1}'.format(
                self.config['id'],
                sha1(str(datetime.datetime.utcnow())).hexdigest()[-16:])
            util.register_checkpoint(self.workdir, 'id', self.taskid)
        else:
            self.taskid = util.checkpoint(self.workdir, 'id')
            util.register_checkpoint(self.workdir, 'RESTARTED', str(datetime.datetime.utcnow()))

        self.config = apply_matching(self.config)
        for cfg in self.config['tasks']:
            label = cfg['label']
            self.extra_inputs[label] = map(
                    partial(util.findpath, self.basedirs),
                    cfg.get('extra inputs', []))
            self.outputs[label] = cfg.get('outputs', [])
            self.args[label] = cfg.get('parameters', [])
            self.outputformats[label] = cfg.get("output format", "{base}_{id}.{ext}")
            self.cmds[label] = cfg.get('cmd')

            taskdir = os.path.join(self.workdir, label)
            stageoutdir = os.path.join(self.stageout, label)
            if create:
                if not os.path.exists(taskdir):
                    os.makedirs(taskdir)
                if chirp_root and stageoutdir.startswith(chirp_root):
                    target = stageoutdir.replace(chirp_root, '', 1)
                    if not chirp.exists(chirp_server, chirp_root, target):
                        chirp.makedirs(chirp_server, chirp_root, target)
                else:
                    if not fs.exists(stageoutdir):
                        fs.makedirs(stageoutdir)

                shutil.copy(self.config['filename'], os.path.join(self.workdir, 'lobster_config.yaml'))

        for p in (self.parrot_bin, self.parrot_lib):
            if not os.path.exists(p):
                os.makedirs(p)

        for exe in ('parrot_run', 'chirp', 'chirp_put', 'chirp_get'):
            shutil.copy(util.which(exe), self.parrot_bin)
            subprocess.check_call(["strip", os.path.join(self.parrot_bin, exe)])

        p_helper = os.path.join(os.path.dirname(self.parrot_path), 'lib', 'lib64', 'libparrot_helper.so')
        shutil.copy(p_helper, self.parrot_lib)

    def get_jobdir(self, jobid, label='', status='running'):
        # See id2dir for job id formatting in filesystem paths
        return os.path.normpath(os.path.join(self.workdir, label, status, util.id2dir(jobid)))

    def create_jobdir(self, jobid, label, status='running'):
        jdir = self.get_jobdir(jobid, label, status)
        if not os.path.isdir(jdir):
            os.makedirs(jdir)
        return jdir

    def move_jobdir(self, jobid, label, status, oldstatus='running'):
        """Moves a job parameter/log directory from one status directory to
        another.

        Returns the new directory.
        """
        # See above for job id splitting.  Moves directories and removes
        # old empty directories.
        old = self.get_jobdir(jobid, label, oldstatus)
        new = self.get_jobdir(jobid, label, status)
        parent = os.path.dirname(new)
        if not os.path.isdir(parent):
            os.makedirs(parent)
        shutil.move(old, parent)
        if len(os.listdir(os.path.dirname(old))) == 0:
            os.removedirs(os.path.dirname(old))
        return new

    def get_jobids(self, label, status='running'):
        # Iterates over the job directories and returns all jobids found
        # therein.
        parent = os.path.join(self.workdir, label, status)
        for d in glob.glob(os.path.join(parent, '*', '*')):
            yield int(os.path.relpath(d, parent).replace(os.path.sep, ''))

    def done(self):
        raise NotImplementedError

    def obtain(self):
        raise NotImplementedError

    def release(self, tasks):
        raise NotImplementedError

    def terminate(self):
        raise NotImplementedError

    def work_left(self):
        raise NotImplementedError

    def retry(self, fct, args, kwargs, attempts=10):
        while attempts > 0:
            attempts -= 1

            try:
                return fct(*args, **kwargs)
            except sqlite3.OperationalError:
                logger.critical("failed to perform SQL operation.  {0} attempts remaining.".format(attempts))
                if attempts <= 0:
                    raise
                time.sleep(1)


