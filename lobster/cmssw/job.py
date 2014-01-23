import imp
import os
import pickle
import shutil
import sqlite3
import time

import lobster.job
import sandbox
from dataset import DASInterface, FileInterface
from jobit import SQLInterface as JobitStore

from FWCore.PythonUtilities.LumiList import LumiList
from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface

class JobProvider(lobster.job.JobProvider):
    def __init__(self, config):
        self.__config = config

        self.__workdir = config['workdir']
        self.__stageout = config['stageout location']
        self.__sandbox = os.path.join(self.__workdir, 'sandbox')

        self.__datasets = {}
        self.__configs = {}
        self.__args = {}
        self.__jobdirs = {}
        self.__stageoutdirs = {}
        self.__outputs = {}

        if 'files' in repr(config):
            ds_interface = FileInterface(config)
        else:
            ds_interface = DASInterface(config)

        create = not os.path.exists(self.__workdir)
        if create:
            os.makedirs(self.__sandbox)
            for fn in ['job.py']:
                shutil.copy(os.path.join(os.path.dirname(__file__), 'data', fn),
                            os.path.join(self.__sandbox, fn))
            blacklist = config.get('sandbox blacklist', [])
            sandbox.package(os.environ['LOCALRT'], self.__sandbox, blacklist)

        for cfg in config['tasks']:
            label = cfg['dataset label']
            cms_config = cfg['cmssw config']

            self.__datasets[label] = cfg['dataset']
            self.__configs[label] = os.path.basename(cms_config)
            self.__args[label] = cfg.get('parameters', [])
            self.__outputs[label] = []

            if cfg.has_key('outputs'):
                self.__outputs[label].extend(cfg['outputs'])
            else:
                with open(cms_config, 'r') as f:
                    source = imp.load_source('cms_config_source', cms_config, f)
                    cfg_interface = CfgInterface(source.process)
                    for m in cfg_interface.data.outputModules:
                        self.__outputs[label].append(getattr(cfg_interface.data, m).fileName._value)

            taskdir = os.path.join(self.__workdir, label)
            stageoutdir = os.path.join(self.__stageout, taskdir)
            if create:
                for dir in [taskdir, stageoutdir]:
                    if not os.path.exists(dir):
                        os.makedirs(dir)

                shutil.copy(cms_config, os.path.join(taskdir, os.path.basename(cms_config)))
            elif os.path.exists(os.path.join(taskdir, 'running')):
                for d in os.listdir(os.path.join(taskdir, 'running')):
                    shutil.move(os.path.join(taskdir, 'running', d), os.path.join(taskdir, 'failed'))

        self.__store = JobitStore(config)
        if create:
            self.__store.register_jobits(ds_interface)
        else:
            self.__store.reset_jobits()

    def obtain(self, num=1):
        res = self.retry(self.__store.pop_jobits, ([20] * num,), {})
        if not res:
            return None

        tasks = []
        ids = []

        for (id, label, files, lumis) in res:
            ids.append(id)

            config = self.__configs[label]
            args = self.__args[label]

            inputs = [(os.path.join(self.__workdir, label, config), config),
                    (self.__sandbox + ".tar.bz2", "sandbox.tar.bz2"),
                    (os.path.join(os.path.dirname(__file__), 'data', 'wrapper.sh'), 'wrapper.sh')]

            sdir = os.path.join(self.__stageout, label)
            jdir = os.path.join(self.__workdir, label, 'running', id)
            if not os.path.isdir(jdir):
                os.makedirs(jdir)

            with open(os.path.join(jdir, 'parameters.pkl'), 'wb') as f:
                pickle.dump((args, files, lumis), f, pickle.HIGHEST_PROTOCOL)
            inputs.append((os.path.join(jdir, 'parameters.pkl'), 'parameters.pkl'))

            self.__jobdirs[id] = jdir
            outputs = [(os.path.join(sdir, f.replace('.root', '_%s.root' % id)), f) for f in self.__outputs[label]]
            outputs.extend([(os.path.join(jdir, f), f) for f in ['report.xml.gz', 'cmssw.log.gz', 'processed.pkl']])

            cmd = './wrapper.sh python job.py {0} parameters.pkl'.format(config)

            tasks.append((id, cmd, inputs, outputs))

        print "Creating job(s) {0}".format(", ".join(ids))

        return tasks

    def release(self, id, return_code, output):
        failed = (return_code != 0)
        jdir = self.__jobdirs[id]

        try:
            with open(os.path.join(jdir, 'parameters.pkl'), 'rb') as f:
                in_lumis = pickle.load(f)[2]
            with open(os.path.join(jdir, 'processed.pkl'), 'rb') as f:
                out_lumis = pickle.load(f)
            not_processed = (out_lumis - in_lumis).getLumis()
        except Exception as e:
            # FIXME treat this properly
            print "Job", id, "had a problem:", e
            failed = True
            not_processed = in_lumis.getLumis()

        print "Job", id, "returned with exit code", return_code, "missing", len(not_processed), "lumis"

        self.retry(self.__store.update_jobits, (id, failed, not_processed), {})

        with open(os.path.join(jdir, 'job.log'), 'w') as f:
            f.write(output)

        if failed:
            shutil.move(jdir, jdir.replace('running', 'failed'))
        else:
            shutil.move(jdir, jdir.replace('running', 'successful'))

    def done(self):
        return self.__store.unfinished_jobits() == 0

    def work_left(self):
        return self.__store.unfinished_jobits()

    def retry(self, fct, args, kwargs, attempts=10):
        while attempts > 0:
            attempts -= 1

            try:
                return fct(*args, **kwargs)
            except sqlite3.OperationalError:
                print "Failed to perform SQL operation.  {0} attempts remaining.".format(attempts)
                if attempts <= 0:
                    raise
                time.sleep(1)
