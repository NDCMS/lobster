from collections import defaultdict
import gzip
import imp
import json
import multiprocessing
import os
import shutil
import sys

from lobster import chirp, job, util
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

    def __init__(
            self, id, dataset, files, lumis, jobdir,
            cmssw_job=True, empty_source=False, chirp=None, chirp_root=None, merge=False):
        self._id = id
        self._dataset = dataset
        self._files = [(id, file) for id, file in files if file]
        self._use_local = any([run == -1 or lumi == -1 for (id, file, run, lumi) in lumis])
        self._file_based = any([run == -2 or lumi == -2 for (id, file, run, lumi) in lumis]) or self._use_local
        self._jobits = lumis
        self._jobdir = jobdir
        self._outputs = []
        self._merge = merge
        self._cmssw_job = cmssw_job
        self._empty_source = empty_source
        self._chirp = chirp

        chirp_stagein = chirp and self._use_local and all([file.startswith(chirp_root) for (_, file) in self._files])
        self._transfer_inputs = merge or chirp_stagein

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

    @outputs.setter
    def outputs(self, files):
        self._outputs = files

    def get_job_info(self):
        lumis = set([(run, lumi) for (id, file, run, lumi) in self._jobits])
        files = set([filename for (id, filename) in self._files])
        if self._use_local and not self._transfer_inputs:
            files = ['file:' + os.path.basename(f) for f in files]

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
                if self._use_local:
                    file = 'file:' + os.path.basename(file)
                if not self._empty_source:
                    skipped = file in files_skipped or file not in files_info
                    read = 0 if failed or skipped else files_info[file][0]

            if not self._file_based:
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

    def update_inputs(self, inputs):
        if self._use_local and not self._transfer_inputs:
            inputs += [(f, os.path.basename(f), False) for id, f in self._files if f]

    def update_config(self, config):
        if self._transfer_inputs:
            config['transfer inputs'] = True

class JobProvider(job.JobProvider):
    def __init__(self, config):
        super(JobProvider, self).__init__(config)

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

        self.__chirp = self.config.get('stageout server', None)
        self.__chirp_root = self.config.get('chirp root', self.stageout) if self.__chirp else ''
        self.__sandbox = os.path.join(self.workdir, 'sandbox')

        self.__unlinker = chirp.Unlinker(self.stageout, self.__chirp)

        self.__datasets = {}
        self.__configs = {}
        self.__jobhandlers = {}
        self.__interface = MetaInterface()
        self.__store = jobit.JobitStore(self.config)

        self.__grid_files = [(os.path.join('/cvmfs/grid.cern.ch', x), os.path.join('grid', x), True) for x in
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

        if self.config.get('use dashboard', True):
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

    def get_report(self, label, job):
        jobdir = self.get_jobdir(job, label, 'successful')

        return os.path.join(jobdir, 'report.xml.gz')

    def obtain(self, num=1):
        # FIXME allow for adjusting the number of LS per job

        jobinfos = self.retry(self.__store.pop_unmerged_jobs, (self.config.get('merge size', -1), 10), {}) \
                + self.retry(self.__store.pop_jobits, (num,), {})
        if not jobinfos or len(jobinfos) == 0:
            return None

        tasks = []
        ids = []

        for (id, label, files, lumis, unique_arg, empty_source, merge) in jobinfos:
            sdir = os.path.join(self.stageout, label)
            ids.append(id)

            inputs = [(self.__sandbox + ".tar.bz2", "sandbox.tar.bz2", True),
                      (os.path.join(os.path.dirname(__file__), 'data', 'mtab'), 'mtab', True),
                      (os.path.join(os.path.dirname(__file__), 'data', 'siteconfig'), 'siteconfig', True),
                      (os.path.join(os.path.dirname(__file__), 'data', 'wrapper.sh'), 'wrapper.sh', True),
                      (self.parrot_bin, 'bin', None),
                      (self.parrot_lib, 'lib', None)
                      ] + self.__grid_files

            if merge:
                args = ['output=' + self.outputs[label][0]]
                cmssw_job = True
                cms_config = os.path.join(os.path.dirname(__file__), 'data', 'merge_cfg.py')
                inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'merge_reports.py'), 'merge_reports.py', True))

                missing = []
                infiles = []
                inreports = []

                for job, _, _, _ in lumis:
                    report = self.get_report(label, job)
                    base, ext = os.path.splitext(self.outputs[label][0])
                    input = os.path.join(sdir, self.outputformats[label].format(base=base, ext=ext[1:], id=job))

                    # FIXME this is not chirp-proof!!!!111!1111!!!!!elf
                    if os.path.isfile(report) and os.path.isfile(input):
                        inreports.append(report)
                        infiles.append((job, input))
                        # FIXME we can also read files locally, or via
                        # xrootd...
                        if not self.__chirp:
                            inputs.append((input, os.path.basename(input), False))
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
                epilogue = ['python', 'merge_reports.py', 'report.xml.gz'] \
                        + ["_".join(os.path.normpath(r).split(os.sep)[-3:]) for r in inreports]

                files = infiles
            else:
                args = [x for x in self.args[label] + [unique_arg] if x]
                cmssw_job = self.__configs.has_key(label)
                cms_config = None
                prologue = self.config.get('prologue')
                epilogue = None
                if cmssw_job:
                    cms_config = os.path.join(self.workdir, label, self.__configs[label])

            if cmssw_job:
                inputs.extend([(os.path.join(os.path.dirname(__file__), 'data', 'job.py'), 'job.py', True),
                               (cms_config, os.path.basename(cms_config), True)
                               ])

            if 'X509_USER_PROXY' in os.environ:
                inputs.append((os.environ['X509_USER_PROXY'], 'proxy', False))

            inputs += [(i, os.path.basename(i), True) for i in self.extra_inputs[label]]

            jdir = self.create_jobdir(id, label, 'running')

            monitorid, syncid = self.__dash.register_job(id)

            handler = JobHandler(
                id, label, files, lumis, jdir, cmssw_job, empty_source,
                merge=merge,
                chirp=self.__chirp,
                chirp_root=self.__chirp_root)
            files, lumis = handler.get_job_info()

            stageout = []
            stagein = []
            outputs = []
            for filename in self.outputs[label]:
                base, ext = os.path.splitext(filename)
                outname = self.outputformats[label].format(base=base, ext=ext[1:], id=id)

                handler.outputs.append(os.path.join(label, outname))
                prefix = self.stageout.replace(self.__chirp_root, '', 1)
                stageout.append((filename, os.path.join(prefix, label, outname)))
                if not self.__chirp:
                    outputs.append((os.path.join(sdir, outname), filename))

            if not cmssw_job:
                if handler.file_based:
                    args += [','.join([os.path.basename(f) for f in files])]
                cmd = 'sh wrapper.sh {0} {1}'.format(self.cmds[label], ' '.join(args))
            else:
                outputs.extend([(os.path.join(jdir, f), f) for f in ['report.xml.gz', 'cmssw.log.gz', 'report.json']])

                sum = self.config.get('cmssw summary', True)

                config = {
                    'mask': {
                        'files': list(files),
                        'lumis': lumis.getCompactList() if lumis else None
                    },
                    'monitoring': {
                        'monitorid': monitorid,
                        'syncid': syncid,
                        'taskid': self.taskid
                    },
                    'arguments': args,
                    'chirp root': self.__chirp_root,
                    'chirp server': self.__chirp,
                    'output files': stageout,
                    'want summary': sum
                }
                if prologue:
                    config['prologue'] = prologue

                if epilogue:
                    config['epilogue'] = epilogue

                handler.update_config(config)
                handler.update_inputs(inputs)

                with open(os.path.join(jdir, 'parameters.json'), 'w') as f:
                    json.dump(config, f, indent=2)
                inputs.append((os.path.join(jdir, 'parameters.json'), 'parameters.json', False))

                cmd = 'sh wrapper.sh python job.py {0} parameters.json'.format(os.path.basename(cms_config))

            tasks.append((id, cmd, inputs, outputs))

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
                except (ValueError, EOFError, IOError) as e:
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
                cleanup += handler.outputs
            else:
                self.move_jobdir(handler.id, handler.dataset, 'successful')

            self.__dash.update_job(task.tag, dash.RETRIEVED)

            jobs[(handler.dataset, handler.jobit_source)].append((job_update, file_update, jobit_update))

            del self.__jobhandlers[task.tag]

        self.__dash.free()

        if len(cleanup) > 0:
            self.__unlinker.remove(cleanup)
        if len(jobs) > 0:
            self.retry(self.__store.update_jobits, (jobs,), {})

    def done(self):
        left = self.__store.unfinished_jobits()
        if self.config.get('merge size', -1) > 0:
            return self.__store.merged() and left == 0
        return left == 0

    def work_left(self):
        return self.__store.unfinished_jobits()

