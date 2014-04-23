import datetime
import gzip
import imp
import os
import pickle
import re
import shutil
import sqlite3
import time
import sys

from hashlib import sha1

import lobster.job
import dash
import sandbox
from jobit import SQLInterface as JobitStore

from FWCore.PythonUtilities.LumiList import LumiList
from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface
from ProdCommon.FwkJobRep.ReportParser import readJobReport

class JobProvider(lobster.job.JobProvider):
    def __init__(self, config):
        self.__config = config

        self.__workdir = config['workdir']
        self.__stageout = config['stageout location']
        self.__sandbox = os.path.join(self.__workdir, 'sandbox')

        self.__datasets = {}
        self.__configs = {}
        self.__extra_inputs = {}
        self.__args = {}
        self.__jobdirs = {}
        self.__jobdatasets = {}
        self.__outputs = {}

        create = not os.path.exists(self.__workdir)
        if create:
            blacklist = config.get('sandbox blacklist', [])
            sandbox.package(os.environ['LOCALRT'], self.__sandbox, blacklist, config.get('recycle sandbox'))

        statusfile = os.path.join(self.__workdir, 'status.pkl')
        if not os.path.exists(statusfile):
            self.__taskid = 'lobster_{0}_{1}'.format(
                    self.__config['id'],
                    sha1(str(datetime.datetime.utcnow())).hexdigest()[-16:])
            with open(statusfile, 'wb') as f:
                pickle.dump(self.__taskid, f, pickle.HIGHEST_PROTOCOL)
        else:
            with open(statusfile, 'rb') as f:
                self.__taskid = pickle.load(f)

        if config.get('use dashboard', False):
            print "Using Dashboard with task", self.__taskid
            self.__dash = dash.Monitor(self.__taskid)
        else:
            self.__dash = dash.DummyMonitor(self.__taskid)

        defaults = config.get('task defaults', {})
        matching = defaults.get('matching', [])

        self.__store = JobitStore(config)
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

            cms_config = cfg['cmssw config']

            self.__datasets[label] = cfg['dataset']
            self.__configs[label] = os.path.basename(cms_config)
            self.__extra_inputs[label] = cfg.get('extra inputs', [])
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
                    else:
                        # TODO warn about non-empty stageout directories
                        pass

                shutil.copy(cms_config, os.path.join(taskdir, os.path.basename(cms_config)))
                shutil.copy(config['filepath'], os.path.join(self.__workdir, 'lobster_config.yaml'))

                self.__store.register_jobits(cfg)
            elif os.path.exists(os.path.join(taskdir, 'running')):
                for d in os.listdir(os.path.join(taskdir, 'running')):
                    shutil.move(os.path.join(taskdir, 'running', d), os.path.join(taskdir, 'failed'))

        if create:
            self.__dash.register_run()
        else:
            for id in self.__store.reset_jobits():
                self.__dash.update_job(id, dash.ABORTED)

    def obtain(self, num=1, bijective=False):
        # FIXME allow for adjusting the number of LS per job
        res = self.retry(self.__store.pop_jobits, (num, bijective), {})
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
                      (os.path.join(os.path.dirname(__file__), 'data', 'wrapper.sh'), 'wrapper.sh'),
                      (os.path.join(os.path.dirname(__file__), 'data', 'job.py'), 'job.py')
                      ]

            if 'X509_USER_PROXY' in os.environ:
                inputs.append((os.environ['X509_USER_PROXY'], 'proxy'))

            inputs += [(i, os.path.basename(i)) for i in self.__extra_inputs[label]]

            sdir = os.path.join(self.__stageout, label)
            jdir = os.path.join(self.__workdir, label, 'running', id)
            if not os.path.isdir(jdir):
                os.makedirs(jdir)

            monitorid, syncid = self.__dash.register_job(id)

            with open(os.path.join(jdir, 'parameters.pkl'), 'wb') as f:
                pickle.dump((args, files, lumis, self.__taskid, monitorid, syncid), f, pickle.HIGHEST_PROTOCOL)
            inputs.append((os.path.join(jdir, 'parameters.pkl'), 'parameters.pkl'))

            self.__jobdirs[id] = jdir
            self.__jobdatasets[id] = label
            outputs = [(os.path.join(sdir, f.replace('.root', '_%s.root' % id)), f) for f in self.__outputs[label]]
            outputs.extend([(os.path.join(jdir, f), f) for f in ['report.xml.gz', 'cmssw.log.gz', 'report.pkl']])

            cmd = './wrapper.sh python job.py {0} parameters.pkl'.format(config)

            tasks.append((id, cmd, inputs, outputs))

        print "Creating job(s) {0}".format(", ".join(ids))

        self.__dash.free()

        return tasks

    def release(self, tasks):
        jobs = []
        for task in tasks:
            failed = (task.return_status != 0)
            jdir = self.__jobdirs[task.tag]
            dset = self.__jobdatasets[task.tag]

            if task.output:
                f = gzip.open(os.path.join(jdir, 'job.log.gz'), 'wb')
                f.write(task.output)
                f.close()

            try:
                with open(os.path.join(jdir, 'report.pkl'), 'rb') as f:
                    out_lumis, task_times = pickle.load(f)
            except (EOFError, IOError) as e:
                print e
                failed = True
                out_lumis = LumiList()
                task_times = [None] * 6

            try:
                with open(os.path.join(jdir, 'parameters.pkl'), 'rb') as f:
                    in_lumis = pickle.load(f)[2]
                not_processed = (in_lumis - out_lumis).getLumis()
                processed = out_lumis.getLumis()
            except (EOFError, IOError) as e:
                print e
                # FIXME treat this properly
                failed = True
                not_processed = in_lumis.getLumis()
                processed = []

            print "Job", task.tag, "returned with exit code", task.return_status

            submissions = task.total_submissions
            times = [
                    task.submit_time / 1000000,
                    task.send_input_start / 1000000,
                    task.send_input_finish / 1000000
                    ] + task_times + [
                    task.receive_output_start / 1000000,
                    task.receive_output_finish / 1000000,
                    task.finish_time / 1000000,
                    task.cmd_execution_time / 1000000,
                    task.total_cmd_execution_time / 1000000,
                    ]
            data = [task.total_bytes_received, task.total_bytes_sent]

            processed_events = 0
            if failed:
                shutil.move(jdir, jdir.replace('running', 'failed'))
            else:
                f = gzip.open(os.path.join(jdir, 'report.xml.gz'), 'r')
                for report in readJobReport(f)[0].files:
                    processed_events += int(report['TotalEvents'])
                f.close()
                shutil.move(jdir, jdir.replace('running', 'successful'))

            self.__dash.update_job(task.tag, dash.DONE)

            jobs.append([task.tag, dset, task.hostname, failed, task.return_status, submissions, processed, not_processed, times, data, processed_events])

        self.__dash.free()

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
