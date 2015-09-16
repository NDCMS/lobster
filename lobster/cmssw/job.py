from collections import defaultdict
import gzip
import imp
import json
import multiprocessing
import os
import re
import shutil
import subprocess
import sys

from lobster import fs, job, util
import dash
import sandbox

import jobit
from dataset import MetaInterface

from FWCore.PythonUtilities.LumiList import LumiList

import work_queue as wq

logger = multiprocessing.get_logger()

class JobHandler(object):
    """
    Handles mapping of lumi sections to files etc.
    """

    def __init__(
            self, id, dataset, files, lumis, jobdir,
            cmssw_job=True, empty_source=False, merge=False, local=False):
        self._id = id
        self._dataset = dataset
        self._files = [(id, file) for id, file in files]
        self._file_based = any([run < 0 or lumi < 0 for (id, file, run, lumi) in lumis])
        self._jobits = lumis
        self._jobdir = jobdir
        self._outputs = []
        self._merge = merge
        self._cmssw_job = cmssw_job
        self._empty_source = empty_source
        self._local = local

    @property
    def cmssw_job(self):
        return self._cmssw_job

    @property
    def dataset(self):
        return self._dataset

    @property
    def id(self):
        return self._id

    @property
    def jobdir(self):
        return self._jobdir

    @jobdir.setter
    def jobdir(self, dir):
        self._jobdir = dir

    @property
    def outputs(self):
        return self._outputs

    @property
    def file_based(self):
        return self._file_based

    @property
    def jobit_source(self):
        return 'jobs' if self._merge else 'jobits_' + self._dataset

    @property
    def merge(self):
        return self._merge

    @outputs.setter
    def outputs(self, files):
        self._outputs = files

    def get_job_info(self):
        lumis = set([(run, lumi) for (id, file, run, lumi) in self._jobits])
        files = set([filename for (id, filename) in self._files if filename])

        if self._file_based:
            lumis = None
        else:
            lumis = LumiList(lumis=lumis)

        return files, lumis

    def get_jobit_info(self, failed, files_info, files_skipped, events_written):
        events_read = 0
        file_update = []
        jobit_update = []

        jobits_processed = 0
        jobits_missed = 0

        for (id, file) in self._files:
            file_jobits = [tpl for tpl in self._jobits if tpl[1] == id]

            skipped = False
            read = 0
            if self._cmssw_job:
                if not self._empty_source:
                    skipped = file in files_skipped or file not in files_info
                    read = 0 if failed or skipped else files_info[file][0]

            events_read += read

            if not failed:
                if skipped:
                    for (lumi_id, lumi_file, r, l) in file_jobits:
                        jobit_update.append((jobit.FAILED, lumi_id))
                        jobits_missed += 1
                elif not self._file_based:
                    file_lumis = set(map(tuple, files_info[file][1]))
                    for (lumi_id, lumi_file, r, l) in file_jobits:
                        if (r, l) not in file_lumis:
                            jobit_update.append((jobit.FAILED, lumi_id))
                            jobits_missed += 1

            file_update.append((read, 1 if skipped else 0, id))

        if not self._file_based:
            jobits_missed = len(self._jobits) if failed else jobits_missed
        else:
            jobits_missed = len(self._files) - len(files_info.keys())

        if failed:
            events_written = 0
            status = jobit.FAILED
        else:
            status = jobit.SUCCESSFUL

        if self._merge:
            file_update = []
            # FIXME not correct
            jobits_missed = 0

        return [jobits_missed, events_read, events_written, status], \
                file_update, jobit_update

    def update_job(self, parameters, inputs, outputs, se):
        local = self._local or self._merge
        se.preprocess(parameters, self._merge)
        if local and se.transfer_inputs():
            inputs += [(se.local(f), os.path.basename(f), False) for id, f in self._files if f]
        if se.transfer_outputs():
            outputs += [(se.local(rf), os.path.basename(lf)) for lf, rf in self._outputs]

class JobProvider(job.JobProvider):
    def __init__(self, config, interval=300):
        super(JobProvider, self).__init__(config)

        self.bad_exitcodes += [169]
        self.__interval = interval  # seconds
        self.__dash = None
        self.__dash_checker = dash.JobStateChecker(interval)

        if 'merge size' in self.config:
            bytes = self.config['merge size']
            orig = bytes
            if isinstance(bytes, basestring):
                unit = bytes[-1].lower()
                try:
                    bytes = float(bytes[:-1])
                    if unit == 'k':
                        bytes *= 1000
                    elif unit == 'm':
                        bytes *= 1e6
                    elif unit == 'g':
                        bytes *= 1e9
                    else:
                        bytes = -1
                except ValueError:
                    bytes = -1
                self.config['merge size'] = bytes

            if bytes > 0:
                logger.info('merging outputs up to {0} bytes'.format(bytes))
            else:
                logger.error('merging disabled due to malformed size {0}'.format(orig))

        self.__sandbox = os.path.join(self.workdir, 'sandbox')

        self.__events = {}
        self.__configs = {}
        self.__local = {}
        self.__edm_outputs = {}
        self.__jobhandlers = {}
        self.__interface = MetaInterface()
        self.__store = jobit.JobitStore(self.config)

        if not util.checkpoint(self.workdir, 'executable'):
            # We can actually have more than one exe name (one per task label)
            # Set 'cmsRun' if any of the tasks are of that type,
            # or use cmd command if all tasks execute the same cmd,
            # or use 'noncmsRun' if task cmds are different
            # Using this for dashboard exe name reporting
            cmsconfigs = [cfg.get('cmssw config') for cfg in self.config['tasks']]
            cmds = [cfg.get('cmd') for cfg in self.config['tasks']]
            if any(cmsconfigs):
                exename = 'cmsRun'
            elif all(x == cmds[0] and x is not None for x in cmds):
                exename = cmds[0]
            else:
                exename = 'noncmsRun'

            util.register_checkpoint(self.workdir, 'executable', exename)

        if self.config.get('use dashboard', True):
            logger.info("using dashboard with task id {0}".format(self.taskid))
            monitor = dash.Monitor
        else:
            monitor = dash.DummyMonitor

        if not util.checkpoint(self.workdir, 'sandbox'):
            blacklist = self.config.get('sandbox blacklist', [])
            cmssw_version = sandbox.package(os.environ['LOCALRT'], self.__sandbox,
                                            blacklist, self.config.get('recycle sandbox'))
            util.register_checkpoint(self.workdir, 'sandbox', 'CREATED')
            util.register_checkpoint(self.workdir, 'sandbox cmssw version', cmssw_version)
            self.__dash = monitor(self.workdir)
            self.__dash.register_run()

        else:
            self.__dash = monitor(self.workdir)
            for id in self.__store.reset_jobits():
                self.__dash.update_job(id, dash.ABORTED)

        for cfg in self.config['tasks']:
            label = cfg['label']
            cfg['basedirs'] = self.basedirs

            cms_config = cfg.get('cmssw config')
            if cms_config:
                self.__configs[label] = os.path.basename(cms_config)

            self.__local[label] = cfg.get('local', 'files' in cfg)
            self.__events[label] = cfg.get('events per job', -1)

            # Record whether we'll be handling this output as EDM or not:
            self.__edm_outputs[label] = cfg.get('edm output', True)

            if cms_config and not cfg.has_key('outputs'):
                # To avoid problems loading configs that use the VarParsing module
                sys.argv = ["pacify_varparsing.py"]
                with open(util.findpath(self.basedirs, cms_config), 'r') as f:
                    source = imp.load_source('cms_config_source', cms_config, f)
                    process = source.process
                    if hasattr(process, 'GlobalTag') and hasattr(process.GlobalTag.globaltag, 'value'):
                        cfg['global tag'] = process.GlobalTag.globaltag.value()
                    for label, module in process.outputModules.items():
                        self.outputs[label].append(module.fileName.value())
                    if 'TFileService' in process.services:
                        self.outputs[label].append(process.services['TFileService'].fileName.value())
                        self.__edm_outputs[label] = False

                    logger.info("workflow {0}: adding output file(s) '{1}'".format(label, ', '.join(self.outputs[label])))

            taskdir = os.path.join(self.workdir, label)
            if not util.checkpoint(self.workdir, label):
                if cms_config:
                    shutil.copy(util.findpath(self.basedirs, cms_config), os.path.join(taskdir, os.path.basename(cms_config)))

                logger.info("querying backend for {0}".format(label))
                with fs.default():
                    dataset_info = self.__interface.get_info(cfg)

                if 'filename transformation' in cfg:
                    match, sub = cfg['filename transformation']
                    def trafo(filename):
                        return re.sub(match, sub, filename)
                else:
                    trafo = lambda s: s

                logger.info("registering {0} in database".format(label))
                self.__store.register(cfg, dataset_info, trafo)
                util.register_checkpoint(self.workdir, label, 'REGISTERED')

            elif os.path.exists(os.path.join(taskdir, 'running')):
                for id in self.get_jobids(label):
                    self.move_jobdir(id, label, 'failed')

    def get_report(self, label, job):
        jobdir = self.get_jobdir(job, label, 'successful')

        return os.path.join(jobdir, 'report.json')

    def obtain(self, num=1):
        # FIXME allow for adjusting the number of LS per job

        jobinfos = self.retry(self.__store.pop_unmerged_jobs, (self.config.get('merge size', -1), 10), {}) \
                + self.retry(self.__store.pop_jobits, (num,), {})
        if not jobinfos or len(jobinfos) == 0:
            return None

        tasks = []
        ids = []

        for (id, label, files, lumis, unique_arg, empty_source, merge) in jobinfos:
            ids.append(id)

            jdir = self.create_jobdir(id, label, 'running')

            outputs = []
            inputs = [(self.__sandbox + ".tar.bz2", "sandbox.tar.bz2", True),
                      (os.path.join(os.path.dirname(__file__), 'data', 'siteconfig'), 'siteconfig', True),
                      (os.path.join(os.path.dirname(__file__), 'data', 'wrapper.sh'), 'wrapper.sh', True),
                      (self.parrot_bin, 'bin', None),
                      (self.parrot_lib, 'lib', None)
                      ]

            # Files to make the job wrapper work without referencing WMCore
            # from somewhere else
            import WMCore
            base = os.path.dirname(WMCore.__file__)
            reqs = [
                    "Services/Dashboard/DashboardAPI.pyc",
                    "Services/Dashboard/apmon.pyc",
                    "FwkJobReport"
                    ]
            for f in reqs:
                inputs.append((os.path.join(base, f), os.path.join("python", "WMCore", f), True))

            if merge:
                if not self.__edm_outputs[label]:
                    cmd = 'hadd'
                    args = ['-f', self.outputs[label][0]]
                    cmssw_job = False
                    cms_config = None
                else:
                    cmd = 'cmsRun'
                    args = ['output=' + self.outputs[label][0]]
                    cmssw_job = True
                    cms_config = os.path.join(os.path.dirname(__file__), 'data', 'merge_cfg.py')

                inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'merge_reports.py'), 'merge_reports.py', True))
                inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'job.py'), 'job.py', True))

                missing = []
                infiles = []
                inreports = []

                for job, _, _, _ in lumis:
                    report = self.get_report(label, job)
                    base, ext = os.path.splitext(self.outputs[label][0])
                    input = os.path.join(label, self.outputformats[label].format(base=base, ext=ext[1:], id=job))

                    if os.path.isfile(report):
                        inreports.append(report)
                        infiles.append((job, input))
                        # FIXME we can also read files locally
                        # if not self.__chirp and not self.__xrootd:
                            # inputs.append((input, os.path.basename(input), False))
                    else:
                        missing.append(job)

                if len(missing) > 0:
                    template = "the following have been marked as failed because their output could not be found: {0}"
                    logger.warning(template.format(", ".join(map(str, missing))))
                    self.retry(self.__store.update_missing, (missing,), {})

                if len(infiles) <= 1:
                    # FIXME report these back to the database and then skip
                    # them.  Without failing these job ids, accounting of
                    # running jobs is going to be messed up.
                    logger.debug("skipping job {0} with only one input file!".format(id))

                inputs += [(r, "_".join(os.path.normpath(r).split(os.sep)[-3:]), False) for r in inreports]

                prologue = None
                epilogue = ['python', 'merge_reports.py', 'report.json'] \
                        + ["_".join(os.path.normpath(r).split(os.sep)[-3:]) for r in inreports]

                files = infiles
            else:
                cmd = self.cmds[label]
                args = [x for x in self.args[label] + [unique_arg] if x]
                cmssw_job = self.__configs.has_key(label)
                cms_config = None
                prologue = self.config.get('prologue')
                epilogue = None
                if cmssw_job:
                    cmd = 'cmsRun' 
                    cms_config = os.path.join(self.workdir, label, self.__configs[label])

            inputs.extend([(os.path.join(os.path.dirname(__file__), 'data', 'job.py'), 'job.py', True)])

            if cmssw_job:
                inputs.append((cms_config, os.path.basename(cms_config), True))
                outputs.append((os.path.join(jdir, 'report.xml.gz'), 'report.xml.gz'))

            if 'X509_USER_PROXY' in os.environ:
                inputs.append((os.environ['X509_USER_PROXY'], 'proxy', False))

            inputs += [(i, os.path.basename(i), True) for i in self.extra_inputs[label]]

            monitorid, syncid = self.__dash.register_job(id)

            handler = JobHandler(
                id, label, files, lumis, jdir, cmssw_job, empty_source,
                merge=merge,
                local=self.__local[label])
            files, lumis = handler.get_job_info()

            for filename in self.outputs[label]:
                base, ext = os.path.splitext(filename)
                outname = self.outputformats[label].format(base=base, ext=ext[1:], id=id)

                handler.outputs.append((filename, os.path.join(label, outname)))

            outputs.extend([(os.path.join(jdir, f), f) for f in ['executable.log.gz', 'report.json']])

            sum = self.config.get('cmssw summary', True)

            config = {
                'mask': {
                    'files': list(files),
                    'lumis': lumis.getCompactList() if lumis else None,
                    'events': -1 if merge else self.__events[label],
                },
                'monitoring': {
                    'monitorid': monitorid,
                    'syncid': syncid,
                    'taskid': self.taskid
                },
                'arguments': args,
                'output files': handler.outputs,
                'want summary': sum,
                'executable': cmd,
                'pset': os.path.basename(cms_config) if cms_config else None
            }

            if merge and not self.__edm_outputs[label]:
                config['append inputs to args'] = True

            if prologue:
                config['prologue'] = prologue

            if epilogue:
                config['epilogue'] = epilogue

            handler.update_job(config, inputs, outputs, self._storage)

            with open(os.path.join(jdir, 'parameters.json'), 'w') as f:
                json.dump(config, f, indent=2)
            inputs.append((os.path.join(jdir, 'parameters.json'), 'parameters.json', False))

            cmd = 'sh wrapper.sh python job.py parameters.json'

            cores = 1 if merge else self.config.get('cores per job', 1)

            tasks.append((cores, cmd, id, inputs, outputs))

            self.__jobhandlers[id] = handler

        logger.info("creating job(s) {0}".format(", ".join(map(str, ids))))

        self.__dash.free()

        return tasks

    def release(self, tasks):
        cleanup = []
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
            task_times = [None] * 10
            cmssw_exit_code = None
            cputime = 0
            outsize = 0
            outsize_bare = 0
            cache_start_size = 0
            cache_end_size = 0
            cache = None

            try:
                with open(os.path.join(handler.jobdir, 'report.json'), 'r') as f:
                    data = json.load(f)
                    cache_start_size = data['cache']['start size']
                    cache_end_size = data['cache']['end size']
                    cache = data['cache']['type']
                    task_times = data['task timing info']
                    if handler.cmssw_job:
                        files_info = data['files']['info']
                        files_skipped = data['files']['skipped']
                        events_written = data['events written']
                        cmssw_exit_code = data['cmssw exit code']
                        cputime = data['cpu time']
                        outsize = data['output size']
                        outsize_bare = data['output bare size']
            except (ValueError, EOFError, IOError) as e:
                failed = True
                logger.error("error processing {0}:\n{1}".format(task.tag, e))

            if task.result in [wq.WORK_QUEUE_RESULT_STDOUT_MISSING,
                    wq.WORK_QUEUE_RESULT_SIGNAL,
                    wq.WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION,
                    wq.WORK_QUEUE_RESULT_TASK_TIMEOUT]:
                exit_code = task.result
                failed = True
            elif cmssw_exit_code not in (None, 0):
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
                    cache_start_size,
                    cache_end_size,
                    cache,
                    task.total_bytes_received,
                    task.total_bytes_sent,
                    outsize,
                    outsize_bare
                    ]

            job_update, file_update, jobit_update = \
                    handler.get_jobit_info(failed, files_info, files_skipped, events_written)

            job_update = [util.verify_string(task.hostname), exit_code, task.total_submissions] \
                    + times + data + job_update + [task.tag]

            if failed:
                faildir = self.move_jobdir(handler.id, handler.dataset, 'failed')
                logger.info("parameters and logs can be found in {0}".format(faildir))
                cleanup += [lf for rf, lf in handler.outputs]
            else:
                if handler.merge and self.config.get('delete merged', True):
                    files, _ = handler.get_job_info()
                    cleanup += files
                self.move_jobdir(handler.id, handler.dataset, 'successful')

            self.__dash.update_job(task.tag, dash.RETRIEVED)

            jobs[(handler.dataset, handler.jobit_source)].append((job_update, file_update, jobit_update))

            del self.__jobhandlers[task.tag]

        self.__dash.free()

        if len(cleanup) > 0:
            try:
                fs.remove(*cleanup)
            except (IOError, OSError):
                pass
            except ValueError as e:
                logger.error("error removing {0}:\n{1}".format(task.tag, e))

        if len(jobs) > 0:
            self.retry(self.__store.update_jobits, (jobs,), {})

    def terminate(self):
        for id in self.__store.running_jobs():
            self.__dash.update_job(str(id), dash.CANCELLED)

    def done(self):
        left = self.__store.unfinished_jobits()
        if self.config.get('merge size', -1) > 0:
            return self.__store.merged() and left == 0
        return left == 0

    def __update_dashboard(self, queue, exclude_states):
        try:
            self.__dash_checker.update_dashboard_states(self.__dash, queue, exclude_states)
        except:
            logger.warning("Could not update job states to dashboard")

    def update(self, queue):
        # update dashboard status for all unfinished tasks.
        # WAITING_RETRIEVAL is not a valid status in dashboard,
        # so skipping it for now.
        exclude_states = ( dash.DONE, dash.WAITING_RETRIEVAL )
        self.__update_dashboard(queue, exclude_states)

    def work_left(self):
        return self.__store.unfinished_jobits()
