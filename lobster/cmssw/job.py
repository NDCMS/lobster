from collections import defaultdict
import datetime
from functools import partial
import gzip
import imp
import logging
import os
import pickle
import re
import shutil
import sqlite3
import time
import sys

from hashlib import sha1

from lobster import job, util
import dash
import sandbox

import jobit
from dataset import MetaInterface

from FWCore.PythonUtilities.LumiList import LumiList
from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface

class JobHandler(object):
    """
    Handles mapping of lumi sections to files etc.
    """

    def __init__(self, id, dataset, files, lumis, jdir):
        self.__id = id,
        self.__dataset = dataset
        self.__files = files
        self.__file_based = any([run == -1 or lumi == -1 for (id, file, run, lumi) in lumis])
        self.__lumis = lumis
        self.__jobdir = jdir
        self.__outputs = []

    @property
    def dataset(self):
        return self.__dataset

    @property
    def jobdir(self):
        return self.__jobdir

    @jobdir.setter
    def jobdir(self, dir):
        self.__jobdir = dir

    @property
    def outputs(self):
        return self.__outputs

    @outputs.setter
    def outputs(self, files):
        self.__outputs = files

    def get_job_info(self):
        lumis = set([(run, lumi) for (id, file, run, lumi) in self.__lumis])
        files = set([filename for (id, filename) in self.__files])

        if self.__file_based:
            lumis = None
        else:
            lumis = LumiList(lumis=lumis)

        return files, lumis

    def get_jobit_info(self, failed, files_info, files_skipped, events_written):
        events_read = 0
        file_update = []
        lumi_update = []

        processed = set()
        missed = set()

        for (id, file) in self.__files:
            file_lumis = [tpl for tpl in self.__lumis if tpl[1] == id]

            skipped = file in files_skipped or file not in files_info
            read = 0 if skipped or failed else files_info[file][0]

            if not self.__file_based:
                jobits_finished = len(file_lumis)
                jobits_done = 0 if failed or skipped else len(files_info[file][1])
            else:
                jobits_finished = 1
                jobits_done = 0 if failed or skipped else 1

            events_read += read
            file_update.append((jobits_finished, jobits_done, read, 1 if skipped else 0, id))

            if not failed:
                if skipped:
                    for (lumi_id, lumi_file, r, l) in file_lumis:
                        lumi_update.append((jobit.FAILED, lumi_id))
                        missed.add((r, l))
                elif not self.__file_based:
                    for (lumi_id, lumi_file, r, l) in file_lumis:
                        if (r, l) not in files_info[file][1]:
                            lumi_update.append((jobit.FAILED, lumi_id))
                            missed.add((r, l))
                        else:
                            processed.add((r, l))

        if not self.__file_based:
            jobits_processed = len(processed)
            jobits_missed = jobit.unique_lumis(self.__lumis) if failed else len(missed)
        else:
            jobits_processed = len(files_info.keys())
            jobits_missed = len(self.__files) - len(files_info.keys())

        if failed:
            events_written = 0
            status = jobit.FAILED
        elif jobits_missed > 0:
            status = jobit.INCOMPLETE
        else:
            status = jobit.SUCCESSFUL

        return [jobits_processed, jobits_missed, events_read, events_written, status], \
                file_update, lumi_update

class JobProvider(job.JobProvider):
    def __init__(self, config):
        self.__config = config

        self.__basedirs = [config['configdir'], config['startdir']]

        self.__workdir = config['workdir']
        self.__stageout = config['stageout location']
        self.__sandbox = os.path.join(self.__workdir, 'sandbox')

        self.__datasets = {}
        self.__configs = {}
        self.__extra_inputs = {}
        self.__args = {}
        self.__jobhandlers = {}
        self.__outputs = {}
        self.__outputformats = {}

        self.__interface = MetaInterface()

        statusfile = os.path.join(self.__workdir, 'status.pkl')
        create = not os.path.exists(statusfile)
        if create:
            self.__taskid = 'lobster_{0}_{1}'.format(
                    self.__config['id'],
                    sha1(str(datetime.datetime.utcnow())).hexdigest()[-16:])
            with open(statusfile, 'wb') as f:
                pickle.dump(self.__taskid, f, pickle.HIGHEST_PROTOCOL)

            blacklist = config.get('sandbox blacklist', [])
            sandbox.package(os.environ['LOCALRT'], self.__sandbox, blacklist, config.get('recycle sandbox'))
        else:
            with open(statusfile, 'rb') as f:
                self.__taskid = pickle.load(f)

        if config.get('use dashboard', False):
            logging.info("using dashboard with task id {0}".format(self.__taskid))
            self.__dash = dash.Monitor(self.__taskid)
        else:
            self.__dash = dash.DummyMonitor(self.__taskid)

        defaults = config.get('task defaults', {})
        matching = defaults.get('matching', [])

        self.__store = jobit.JobitStore(config)
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

            self.__datasets[label] = cfg.get('dataset', cfg.get('files', ''))
            self.__configs[label] = os.path.basename(cms_config)
            self.__extra_inputs[label] = map(
                    partial(util.findpath, self.__basedirs),
                    cfg.get('extra inputs', []))
            self.__args[label] = cfg.get('parameters', [])
            self.__outputs[label] = []
            self.__outputformats[label] = cfg.get("output format", "{base}_{id}.{ext}")

            self.__parrot_path = os.path.dirname(util.which('parrot_run'))

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

                shutil.copy(util.findpath(self.__basedirs, cms_config), os.path.join(taskdir, os.path.basename(cms_config)))
                shutil.copy(config['filepath'], os.path.join(self.__workdir, 'lobster_config.yaml'))

                logging.info("querying backend for {0}".format(label))
                dataset_info = self.__interface.get_info(cfg)

                if cfg.has_key('lumi mask'):
                    lumi_mask = LumiList(filename=util.findpath(self.__basedirs, cfg['lumi mask']))
                    for file in dataset_info.files:
                        dataset_info.lumis[file] = lumi_mask.filterLumis(dataset_info.lumis[file])

                logging.info("registering {0} in database".format(label))
                self.__store.register(cfg, dataset_info)
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
        jobinfos = self.retry(self.__store.pop_jobits, (num, bijective), {})
        if not jobinfos or len(jobinfos) == 0:
            return None

        tasks = []
        ids = []

        for (id, label, files, lumis) in jobinfos:
            ids.append(id)

            config = self.__configs[label]
            args = self.__args[label]

            inputs = [(os.path.join(self.__workdir, label, config), config),
                      (self.__sandbox + ".tar.bz2", "sandbox.tar.bz2"),
                      (os.path.join(os.path.dirname(__file__), 'data', 'wrapper.sh'), 'wrapper.sh'),
                      (os.path.join(os.path.dirname(__file__), 'data', 'job.py'), 'job.py'),
                      (os.path.join(self.__parrot_path, 'parrot_run'), 'parrot_run'),
                      ]

            if os.path.isfile(os.path.join(self.__parrot_path, 'parrot_helper.so')):
                inputs.append((os.path.join(self.__parrot_path, 'parrot_helper.so'), 'parrot_helper.so'))

            if 'X509_USER_PROXY' in os.environ:
                inputs.append((os.environ['X509_USER_PROXY'], 'proxy'))

            inputs += [(i, os.path.basename(i)) for i in self.__extra_inputs[label]]

            sdir = os.path.join(self.__stageout, label)
            jdir = os.path.join(self.__workdir, label, 'running', id)
            if not os.path.isdir(jdir):
                os.makedirs(jdir)

            monitorid, syncid = self.__dash.register_job(id)

            handler = JobHandler(id, label, files, lumis, jdir)
            files, lumis = handler.get_job_info()

            with open(os.path.join(jdir, 'parameters.pkl'), 'wb') as f:
                pickle.dump((args, files, lumis, self.__taskid, monitorid, syncid), f, pickle.HIGHEST_PROTOCOL)
            inputs.append((os.path.join(jdir, 'parameters.pkl'), 'parameters.pkl'))

            outputs = []
            for filename in self.__outputs[label]:
                base, ext = os.path.splitext(filename)
                outname = self.__outputformats[label].format(base=base, ext=ext[1:], id=id)
                outputs.append((os.path.join(sdir, outname), filename))

            handler.outputs = map(lambda (a, b): a, outputs)
            outputs.extend([(os.path.join(jdir, f), f) for f in ['report.xml.gz', 'cmssw.log.gz', 'report.pkl']])

            cmd = 'sh wrapper.sh python job.py {0} parameters.pkl'.format(config)

            tasks.append((id, cmd, inputs, outputs))

            self.__jobhandlers[id] = handler

        logging.info("creating job(s) {0}".format(", ".join(ids)))

        self.__dash.free()

        return tasks

    def release(self, tasks):
        jobs = defaultdict(list)
        for task in tasks:
            failed = (task.return_status != 0)

            handler = self.__jobhandlers[task.tag]

            self.__dash.update_job(task.tag, dash.DONE)

            if task.output:
                f = gzip.open(os.path.join(handler.jobdir, 'job.log.gz'), 'wb')
                f.write(task.output)
                f.close()

            try:
                with open(os.path.join(handler.jobdir, 'report.pkl'), 'rb') as f:
                    files_info, files_skipped, events_written, task_times, cmssw_exit_code, cputime = pickle.load(f)
            except (EOFError, IOError) as e:
                logging.error("error processing {0}:\n{1}".format(task.tag, e))

                failed = True

                files_info = {}
                files_skipped = []
                events_written = 0
                task_times = [None] * 6
                cmssw_exit_code = None
                cputime = 0

            if cmssw_exit_code not in (None, 0):
                exit_code = cmssw_exit_code
                if exit_code > 0:
                    failed = True
            else:
                exit_code = task.return_status

            logging.info("job {0} returned with exit code {1}".format(task.tag, exit_code))

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
                    cputime
                    ]
            data = [
                    task.total_bytes_received,
                    task.total_bytes_sent,
                    sum(map(os.path.getsize, filter(os.path.isfile, handler.outputs)))
                    ]

            job_update, file_update, lumi_update = \
                    handler.get_jobit_info(failed, files_info, files_skipped, events_written)

            submissions = task.total_submissions
            job_update = [task.hostname, exit_code, task.total_submissions] \
                    + times + data + job_update + [task.tag]

            if failed:
                shutil.move(handler.jobdir, handler.jobdir.replace('running', 'failed'))
                for filename in filter(os.path.isfile, handler.outputs):
                    os.unlink(filename)
            else:
                shutil.move(handler.jobdir, handler.jobdir.replace('running', 'successful'))

            self.__dash.update_job(task.tag, dash.RETRIEVED)

            jobs[handler.dataset].append((job_update, file_update, lumi_update))

            del self.__jobhandlers[task.tag]

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
                logging.critical("failed to perform SQL operation.  {0} attempts remaining.".format(attempts))
                if attempts <= 0:
                    raise
                time.sleep(1)
