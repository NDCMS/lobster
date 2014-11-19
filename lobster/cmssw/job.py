from collections import defaultdict
import gzip
import imp
import json
import multiprocessing
import os
import shutil
import sys

from lobster import job, util
import dash
import sandbox

import jobit
from dataset import MetaInterface

from FWCore.PythonUtilities.LumiList import LumiList
from ProdCommon.CMSConfigTools.ConfigAPI.CfgInterface import CfgInterface

logger = multiprocessing.get_logger()

class JobHandler(object):
    """
    Handles mapping of lumi sections to files etc.
    """

    def __init__(self, id, dataset, files, jobits, jdir, cmssw_job, empty_source):
        self.__id = id
        self.__dataset = dataset
        self.__files = [(id, file) for id, file in files if file]
        self.__use_local = any([run == -1 or lumi == -1 for (id, file, run, lumi) in jobits])
        self.__file_based = any([run == -2 or lumi == -2 for (id, file, run, lumi) in jobits]) or self.__use_local
        self.__jobits = jobits
        self.__jobdir = jdir
        self.__outputs = []
        self.__cmssw_job = cmssw_job
        self.__empty_source = empty_source

    @property
    def cmssw_job(self):
        return self.__cmssw_job

    @property
    def dataset(self):
        return self.__dataset

    @property
    def id(self):
        return self.__id

    @property
    def jobdir(self):
        return self.__jobdir

    @jobdir.setter
    def jobdir(self, dir):
        self.__jobdir = dir

    @property
    def outputs(self):
        return self.__outputs

    @property
    def file_based(self):
        return self.__file_based

    @property
    def use_local(self):
        return self.__use_local

    @outputs.setter
    def outputs(self, files):
        self.__outputs = files

    def get_job_info(self):
        lumis = set([(run, lumi) for (id, file, run, lumi) in self.__jobits])
        files = set([filename for (id, filename) in self.__files])
        localfiles = set([filename for (id, filename) in self.__files])

        if self.__use_local:
            if self.__cmssw_job:
                localfiles = ['file:' + os.path.basename(f) for f in localfiles]
            else:
                localfiles = [os.path.basename(f) for f in localfiles]

        if self.__file_based:
            lumis = None
        else:
            lumis = LumiList(lumis=lumis)

        return files, localfiles, lumis

    def get_jobit_info(self, failed, files_info, files_skipped, events_written):
        events_read = 0
        file_update = []
        jobit_update = []

        jobits_processed = 0
        jobits_missed = 0
        for (id, file) in self.__files:
            file_jobits = [tpl for tpl in self.__jobits if tpl[1] == id]

            skipped = False
            read = 0
            if self.__cmssw_job:
                if self.__use_local:
                    file = 'file:' + os.path.basename(file)
                if not self.__empty_source:
                    skipped = file in files_skipped or file not in files_info
                    read = 0 if failed or skipped else files_info[file][0]

            if not self.__file_based:
                jobits_finished = len(file_jobits)
                jobits_done = 0 if failed or skipped else len(files_info[file][1])
            else:
                jobits_finished = 1
                jobits_done = 0 if failed or skipped else 1

            events_read += read

            if not failed:
                if skipped:
                    for (lumi_id, lumi_file, r, l) in file_jobits:
                        jobit_update.append((jobit.FAILED, lumi_id))
                        jobits_missed += 1
                elif not self.__file_based:
                    file_lumis = set(map(tuple, files_info[file][1]))
                    for (lumi_id, lumi_file, r, l) in file_jobits:
                        if (r, l) not in file_lumis:
                            jobit_update.append((jobit.FAILED, lumi_id))
                            jobits_missed += 1

            file_update.append((read, 1 if skipped else 0, id))

        if not self.__file_based:
            jobits_missed = len(self.__jobits) if failed else jobits_missed
        else:
            jobits_missed = len(self.__files) - len(files_info.keys())

        if failed:
            events_written = 0
            status = jobit.FAILED
        elif jobits_missed > 0:
            status = jobit.INCOMPLETE
        else:
            status = jobit.SUCCESSFUL

        return [jobits_missed, events_read, events_written, status], \
                file_update, jobit_update

class JobProvider(job.JobProvider):
    def __init__(self, config):
        super(JobProvider, self).__init__(config)

        self.__chirp = self.config.get('stageout server', None)
        self.__sandbox = os.path.join(self.workdir, 'sandbox')

        self.__datasets = {}
        self.__configs = {}
        self.__jobhandlers = {}
        self.__interface = MetaInterface()
        self.__store = jobit.JobitStore(self.config)

        self.__grid_files = [(os.path.join('/cvmfs/grid.cern.ch', x), os.path.join('grid', x)) for x in
                                 ['3.2.11-1/external/etc/profile.d/clean-grid-env-funcs.sh',
                                  '3.2.11-1/external/etc/profile.d/grid-env-funcs.sh',
                                  '3.2.11-1/external/etc/profile.d/grid-env.sh',
                                  '3.2.11-1/etc/profile.d/grid-env.sh',
                                  '3.2.11-1/glite/bin/voms-proxy-info',
                                  '3.2.11-1/glite/lib64/libvomsapi_nog.so.0.0.0',
                                  '3.2.11-1/glite/lib64/libvomsapi_nog.so.0',
                                  'etc/grid-security/certificates'
                                  ]
                             ]

        if self.config.get('use dashboard', False):
            logger.info("using dashboard with task id {0}".format(self.taskid))
            self.__dash = dash.Monitor(self.taskid)
        else:
            self.__dash = dash.DummyMonitor(self.taskid)

        if not util.checkpoint(self.workdir, 'sandbox'):
            blacklist = self.config.get('sandbox blacklist', [])
            sandbox.package(os.environ['LOCALRT'], self.__sandbox, blacklist, self.config.get('recycle sandbox'))
            util.register_checkpoint(self.workdir, 'sandbox', 'CREATED')
            self.__dash.register_run()

        else:
            for id in self.__store.reset_jobits():
                self.__dash.update_job(id, dash.ABORTED)

        for cfg in self.config['tasks']:
            label = cfg['label']
            cfg['basedirs'] = self.basedirs

            cms_config = cfg.get('cmssw config')
            if cms_config:
                self.__configs[label] = os.path.basename(cms_config)

            self.__datasets[label] = cfg.get('dataset', cfg.get('files', ''))

            if cms_config and not cfg.has_key('outputs'):
                sys.argv = [sys.argv[0]] #To avoid problems loading configs that use the VarParsing module
                with open(cms_config, 'r') as f:
                    source = imp.load_source('cms_config_source', cms_config, f)
                    cfg_interface = CfgInterface(source.process)
                    if hasattr(cfg_interface.data.GlobalTag.globaltag, 'value'): #Possibility: make this mandatory?
                        cfg['global tag'] = cfg_interface.data.GlobalTag.globaltag.value()
                    for m in cfg_interface.data.outputModules:
                        self.outputs[label].append(getattr(cfg_interface.data, m).fileName._value)

            taskdir = os.path.join(self.workdir, label)
            if not util.checkpoint(self.workdir, label):
                if cms_config:
                    shutil.copy(util.findpath(self.basedirs, cms_config), os.path.join(taskdir, os.path.basename(cms_config)))

                logger.info("querying backend for {0}".format(label))
                dataset_info = self.__interface.get_info(cfg)

                logger.info("registering {0} in database".format(label))
                self.__store.register(cfg, dataset_info)
                util.register_checkpoint(self.workdir, label, 'REGISTERED')

            elif os.path.exists(os.path.join(taskdir, 'running')):
                for id in self.get_jobids(label):
                    self.move_jobdir(id, label, 'failed')

    def obtain(self, num=1, bijective=False):
        # FIXME allow for adjusting the number of LS per job
        jobinfos = self.retry(self.__store.pop_jobits, (num,), {})
        if not jobinfos or len(jobinfos) == 0:
            return None

        tasks = []
        ids = []

        for (id, label, files, lumis, unique_arg, empty_source) in jobinfos:
            ids.append(id)

            cmssw_job = self.__configs.has_key(label)
            cms_config = self.__configs.get(label)

            inputs = [(self.__sandbox + ".tar.bz2", "sandbox.tar.bz2"),
                      (os.path.join(os.path.dirname(__file__), 'data', 'mtab'), 'mtab'),
                      (os.path.join(os.path.dirname(__file__), 'data', 'siteconfig'), 'siteconfig'),
                      (os.path.join(os.path.dirname(__file__), 'data', 'wrapper.sh'), 'wrapper.sh'),
                      (self.parrot_bin, 'bin'),
                      (self.parrot_lib, 'lib')
                      ] + self.__grid_files

            if cmssw_job:
                inputs.extend([(os.path.join(os.path.dirname(__file__), 'data', 'job.py'), 'job.py'),
                               (os.path.join(self.workdir, label, cms_config), cms_config)
                               ])

            if 'X509_USER_PROXY' in os.environ:
                inputs.append((os.environ['X509_USER_PROXY'], 'proxy'))

            inputs += [(i, os.path.basename(i)) for i in self.extra_inputs[label]]

            sdir = os.path.join(self.stageout, label)
            jdir = self.create_jobdir(id, label, 'running')

            monitorid, syncid = self.__dash.register_job(id)

            handler = JobHandler(id, label, files, lumis, jdir, cmssw_job, empty_source)
            files, localfiles, lumis = handler.get_job_info()

            stageout = []
            stagein = []
            outputs = []
            for filename in self.outputs[label]:
                base, ext = os.path.splitext(filename)
                outname = self.outputformats[label].format(base=base, ext=ext[1:], id=id)

                handler.outputs.append(os.path.join(sdir, outname))
                stageout.append((filename, os.path.join(label, outname)))
                if not self.__chirp:
                    outputs.append((os.path.join(sdir, outname), filename))

            if handler.use_local:
                inputs += [(f, os.path.basename(f)) for f in files]

            args = [x for x in self.args[label] + [unique_arg] if x]
            if not cmssw_job:
                if handler.file_based:
                    args += [','.join(localfiles)]
                cmd = 'sh wrapper.sh {0} {1}'.format(self.cmds[label], ' '.join(args))
            else:
                outputs.extend([(os.path.join(jdir, f), f) for f in ['report.xml.gz', 'cmssw.log.gz', 'report.json']])

                sum = self.config.get('cmssw summary', True)
                with open(os.path.join(jdir, 'parameters.json'), 'w') as f:
                    json.dump({
                        'mask': {
                            'files': list(localfiles),
                            'lumis': lumis.getVLuminosityBlockRange()
                        },
                        'monitoring': {
                            'monitorid': monitorid,
                            'syncid': syncid,
                            'taskid': self.taskid
                        },
                        'arguments': args,
                        'chirp server': self.__chirp,
                        'output files': stageout,
                        'want summary': sum
                    }, f, indent=2)
                inputs.append((os.path.join(jdir, 'parameters.json'), 'parameters.json'))

                cmd = 'sh wrapper.sh python job.py {0} parameters.json'.format(cms_config)

            tasks.append((id, cmd, inputs, outputs))

            self.__jobhandlers[id] = handler

        logger.info("creating job(s) {0}".format(", ".join(ids)))

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

            files_info = {}
            files_skipped = []
            events_written = 0
            task_times = [None] * 7
            cmssw_exit_code = None
            cputime = 0
            outsize = 0

            if handler.cmssw_job:
                try:
                    with open(os.path.join(handler.jobdir, 'report.json'), 'r') as f:
                        data = json.load(f)
                        files_info = data['files']['info']
                        files_skipped = data['files']['skipped']
                        events_written = data['events written']
                        task_times = data['task timing info']
                        cmssw_exit_code = data['cmssw exit code']
                        cputime = data['cpu time']
                        outsize = data['output size']
                except (EOFError, IOError) as e:
                    failed = True
                    logger.error("error processing {0}:\n{1}".format(task.tag, e))

            if cmssw_exit_code not in (None, 0):
                exit_code = cmssw_exit_code
                if exit_code > 0:
                    failed = True
            else:
                exit_code = task.return_status

            logger.info("job {0} returned with exit code {1}".format(task.tag, exit_code))

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
                    outsize
                    ]

            job_update, file_update, jobit_update = \
                    handler.get_jobit_info(failed, files_info, files_skipped, events_written)

            job_update = [util.verify_string(task.hostname), exit_code, task.total_submissions] \
                    + times + data + job_update + [task.tag]

            if failed:
                faildir = self.move_jobdir(handler.id, handler.dataset, 'failed')
                logger.info("parameters and logs can be found in {0}".format(faildir))
            else:
                self.move_jobdir(handler.id, handler.dataset, 'successful')

            self.__dash.update_job(task.tag, dash.RETRIEVED)

            jobs[handler.dataset].append((job_update, file_update, jobit_update))

            del self.__jobhandlers[task.tag]

        self.__dash.free()

        if len(jobs) > 0:
            self.retry(self.__store.update_jobits, (jobs,), {})

    def done(self):
        return self.__store.unfinished_jobits() == 0

    def work_left(self):
        return self.__store.unfinished_jobits()

