import re
import os
import datetime
import itertools
import logging
import glob
import gzip
import shutil
import sqlite3
import subprocess
import time
import yaml

from functools import partial
from hashlib import sha1
from lobster import se, util
from lobster.cmssw import Workflow

logger = logging.getLogger('lobster.job')

def apply_matching(config):
    if 'task defaults' not in config:
        return config
    defaults = config['task defaults']
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
    del config['task defaults']

    return config

class JobProvider(object):
    def __init__(self, config):
        self.config = config
        self.basedirs = [config['base directory'], config['startup directory']]
        self.workdir = config.get('workdir', os.getcwd())
        self._storage = se.StorageConfiguration(config['storage'])
        self._storage.activate()
        self.statusfile = os.path.join(self.workdir, 'status.yaml')

        self.parrot_path = os.path.dirname(util.which('parrot_run'))
        self.parrot_bin = os.path.join(self.workdir, 'bin')
        self.parrot_lib = os.path.join(self.workdir, 'lib')

        self.workflows = {}
        self.bad_exitcodes = config.get('bad exit codes', [])

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
            wflow = Workflow(self.workdir, cfg, self.basedirs)
            self.workflows[wflow.label] = wflow
            if create:
                wflow.create()

        if create:
            self.save_configuration()

        for p in (self.parrot_bin, self.parrot_lib):
            if not os.path.exists(p):
                os.makedirs(p)

        for exe in ('parrot_run', 'chirp', 'chirp_put', 'chirp_get'):
            shutil.copy(util.which(exe), self.parrot_bin)
            subprocess.check_call(["strip", os.path.join(self.parrot_bin, exe)])

        p_helper = os.path.join(os.path.dirname(self.parrot_path), 'lib', 'lib64', 'libparrot_helper.so')
        shutil.copy(p_helper, self.parrot_lib)

    def save_configuration(self):
        with open(os.path.join(self.workdir, 'lobster_config.yaml'), 'w') as f:
            yaml.dump(self.config, f, default_flow_style=False)

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

    def update(self, queue):
        raise NotImplementedError

    def tasks_left(self):
        raise NotImplementedError

    def work_left(self):
        raise NotImplementedError
