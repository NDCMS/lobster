import imp
import os
import pickle
import shutil
import sqlite3
import time
import sys

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
        self.__jobdatasets = {}
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
                sys.argv = [sys.argv[0]] #To avoid problems loading configs that use the VarParsing module
                with open(cms_config, 'r') as f:
                    source = imp.load_source('cms_config_source', cms_config, f)
                    cfg_interface = CfgInterface(source.process)
                    if hasattr(cfg_interface.data.GlobalTag.globaltag, 'value'): #Possibility: make this mandatory?
                        cfg['global tag'] = cfg_interface.data.GlobalTag.globaltag.value()
                    for m in cfg_interface.data.outputModules:
                        self.__outputs[label].append(getattr(cfg_interface.data, m).fileName._value)

            taskdir = os.path.join(self.__workdir, label)
            stageoutdir = os.path.join(self.__stageout, label)
            if create:
                for dir in [taskdir, stageoutdir]:
                    if not os.path.exists(dir):
                        os.makedirs(dir)

                shutil.copy(cms_config, os.path.join(taskdir, os.path.basename(cms_config)))
                shutil.copy(config['filepath'], os.path.join(self.__workdir, 'lobster_config.yaml'))
            elif os.path.exists(os.path.join(taskdir, 'running')):
                for d in os.listdir(os.path.join(taskdir, 'running')):
                    shutil.move(os.path.join(taskdir, 'running', d), os.path.join(taskdir, 'failed'))

        self.__store = JobitStore(config)
        if create:
            self.__store.register_jobits(ds_interface)
        else:
            self.__store.reset_jobits()

    def obtain(self, num=1, bijective=False):
        # FIXME allow for adjusting the number of LS per job
        res = self.retry(self.__store.pop_jobits, ([25] * num, bijective), {})
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
            self.__jobdatasets[id] = label
            outputs = [(os.path.join(sdir, f.replace('.root', '_%s.root' % id)), f) for f in self.__outputs[label]]
            outputs.extend([(os.path.join(jdir, f), f) for f in ['report.xml.gz', 'cmssw.log.gz', 'processed.pkl', 'times.pkl']])

            cmd = './wrapper.sh python job.py {0} parameters.pkl'.format(config)

            tasks.append((id, cmd, inputs, outputs))

        print "Creating job(s) {0}".format(", ".join(ids))

        return tasks

    def release(self, tasks):
        jobs = []
        for task in tasks:
            failed = (task.return_status != 0)
            jdir = self.__jobdirs[task.tag]
            dset = self.__jobdatasets[task.tag]

            if task.output:
                with open(os.path.join(jdir, 'job.log'), 'w') as f:
                    f.write(task.output)

            try:
                with open(os.path.join(jdir, 'parameters.pkl'), 'rb') as f:
                    in_lumis = pickle.load(f)[2]
                with open(os.path.join(jdir, 'processed.pkl'), 'rb') as f:
                    out_lumis = pickle.load(f)
                not_processed = (in_lumis - out_lumis).getLumis()
                processed = out_lumis.getLumis()
            except Exception as e:
                # FIXME treat this properly
                failed = True
                not_processed = in_lumis.getLumis()
                processed = []

            task_times = [None] * 6
            try:
                with open(os.path.join(jdir, 'times.pkl'), 'rb') as f:
                    task_times = pickle.load(f)
            except:
                pass

            print "Job", task.tag, "returned with exit code", task.return_status

            times = [
                    task.submit_time / 1000000,
                    task.send_input_start / 1000000,
                    task.send_input_finish / 1000000
                    ] + task_times + [
                    task.receive_output_start / 1000000,
                    task.receive_output_finish / 1000000,
                    task.finish_time / 1000000
                    ]

            try:
                retries = task.retries
                total_time = task.total_cmd_execution_time
            except:
                retries = -1
                total_time = task.cmd_execution_time

            times += [task.cmd_execution_time, total_time]

            try:
                data = [task.total_bytes_received, task.total_bytes_sent]
            except:
                data = [0, 0]

            jobs.append([task.tag, dset, task.hostname, failed, task.return_status, retries, processed, not_processed, times, data])

            if failed:
                shutil.move(jdir, jdir.replace('running', 'failed'))
            else:
                shutil.move(jdir, jdir.replace('running', 'successful'))
        if len(jobs) > 0:
            self.retry(self.__store.update_jobits, (jobs,), {})

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
