from collections import defaultdict
import gzip
import jobit
import json
import multiprocessing
import os

from IMProv.IMProvDoc import IMProvDoc
from ProdCommon.FwkJobRep.ReportParser import readJobReport
from ProdCommon.FwkJobRep.FwkJobReport import FwkJobReport
from lobster import job, util
import jobit
import dash

logger = multiprocessing.get_logger()

def resolve_name(job, job_type, name, name_format):
    base, ext = os.path.splitext(name)
    id = str(job) if job_type == jobit.PROCESS else 'merged_{0}'.format(job)

    return name_format.format(base=base, ext=ext[1:], id=id)

def resolve_joblist(jobs):
    conjugation = lambda x: 's' if len(x) > 1 else ''

    return 'job{0} {1}'.format(conjugation(jobs), ', '.join([str(j) for j, t in jobs]))

class MergeHandler(object):
    def __init__(self, id, dataset, chirp, jobdir, outname, num_outputs, outname_index, sdir):
        self.__id = id
        self.__dataset = dataset
        self.__chirp = chirp
        self.__jobdir = jobdir
        self.__outname = outname
        self.__sdir = sdir

        self.__reports = set()
        self.__inputs = set()
        self.__jobs = set()

        if num_outputs == 1:
            self.__tag = str(id)
            self.__base = ''
        else:
            base, ext = os.path.splitext(outname)
            self.__tag = '{0}_{1}'.format(id, outname_index)
            self.__base = '{0}_'.format(base)

    @property
    def dataset(self):
        return self.__dataset

    @property
    def reports(self):
        return self.__reports

    @property
    def id(self):
        return self.__id

    @property
    def inputs(self):
        return self.__inputs

    @property
    def jobs(self):
        return self.__jobs

    @property
    def jobdir(self):
        return self.__jobdir

    @property
    def outname(self):
        return self.__outname

    @property
    def base(self):
        return self.__base

    @property
    def tag(self):
        return self.__tag

    def get_job_update(self, failed, outsize):
        """Get update for database.

        Jobs are chosen for merging if their status is successful.
        If the job created to merge a group of successful jobs fails,
        its status is set to failed, while the status of the jobs it
        was trying to merge are returned to successful, so that another merging
        attempt can be made.  Otherwise, the status of both are set to merged.

        """
        if failed:
            merge_job_update = [(jobit.FAILED, outsize, self.__id)]
            success_update = []
            fail_update = [(jobit.SUCCESSFUL, self.__id, job) for job, job_type in self.__jobs]
            jobit_update = [(jobit.SUCCESSFUL, job) for job, job_type in self.__jobs]
            datasets_update = 0
        else:
            merge_job_update = [(jobit.MERGED, outsize, self.__id)]
            success_update = [(jobit.MERGED, self.__id, job) for job, job_type in self.__jobs]
            fail_update = []
            jobit_update = [(jobit.MERGED, job) for job, job_type in self.__jobs]
            datasets_update = len(self.__jobs) + 1

        return [(merge_job_update, success_update, fail_update, jobit_update, datasets_update)]

    def get_job_info(self, stageout_dir):
        args = ['output=' + self.__outname]
        files = ['file:' + os.path.join(stageout_dir, x) for x in self.__inputs]

        return args, files

    def validate(self, report, input):
        return os.path.isfile(report) and os.path.isfile(input)

    def merge_reports(self):
        merged = FwkJobReport()
        for r in self.__reports:
            f = gzip.open(r)
            for report in readJobReport(f):
                merged.inputFiles += report.inputFiles
                if len(merged.files) == 0:
                    merged.files = report.files
                else:
                    for run, lumis in report.files[0]['Runs'].items():
                        if merged.files[0]['Runs'].has_key(run):
                            merged.files[0]['Runs'][run] += lumis
                        else:
                            merged.files[0]['Runs'][run] = lumis
                    merged.files[0]['Runs'].update(report.files[0]['Runs'])
                    events = int(merged.files[0]['TotalEvents']) + int(report.files[0]['TotalEvents'])
                    merged.files[0]['TotalEvents'] = str(events)
            f.close()

        output = IMProvDoc("JobReports")
        output.addNode(merged.save())

        outfile = gzip.open(os.path.join(self.__jobdir, 'report.xml.gz'), 'wb')
        outfile.write(output.makeDOMDocument().toprettyxml())
        outfile.close()

    def cleanup(self):
#         tdir = os.path.dirname(self.__jobdir) # FIXME Do we want to delete old report.xmls?
#         for r in self.__reports:
#             os.remove(r)
        for file in self.__inputs:
            fullpath = os.path.join(self.__sdir, os.path.basename(file))
            os.remove(fullpath)

class MergeProvider(job.JobProvider):
    def __init__(self, config):
        super(MergeProvider, self).__init__(config)

        self.__chirp = self.config.get('stageout server', None)
        self.__sandbox = os.path.join(self.workdir, 'sandbox')
        self.__dash = dash.DummyMonitor(self.taskid)
        self.__missing = []
        self.__mergehandlers = {}

        self.__store = jobit.JobitStore(self.config)
        self.__store.reset_jobits()

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

        self.__common_inputs = [(self.__sandbox + ".tar.bz2", "sandbox.tar.bz2"),
                                (os.path.join(os.path.dirname(__file__), 'data', 'mtab'), 'mtab'),
                                (os.path.join(os.path.dirname(__file__), 'data', 'siteconfig'), 'siteconfig'),
                                (os.path.join(os.path.dirname(__file__), 'data', 'wrapper.sh'), 'wrapper.sh'),
                                (self.parrot_bin, 'bin'),
                                (self.parrot_lib, 'lib'),
                                (os.path.join(os.path.dirname(__file__), 'data', 'job.py'), 'job.py'),
                                (os.path.join(os.path.dirname(__file__), 'data', 'merge_cfg.py'), 'merge_cfg.py')
                                ] + self.__grid_files

        if not util.checkpoint(self.workdir, 'sandbox'):
            raise NotImplementedError

    def get_report(self, label, job):
        jobdir = self.get_jobdir(job, label, 'successful')

        return os.path.join(jobdir, 'report.xml.gz')

    def obtain(self, num=1):
        unmerged_jobs = self.retry(self.__store.pop_unmerged_jobs, (self.config.get('max megabytes', 3500), num), {})
        if not unmerged_jobs or len(unmerged_jobs) == 0:
            return None

        tasks = []
        for merging_job, dset, jobs in unmerged_jobs:
            out_tag = 'merged_{0}'.format(merging_job)

            monitorid, syncid = self.__dash.register_job(merging_job)

            sdir = os.path.join(self.stageout, dset)
            jdir = self.create_jobdir(merging_job, dset, 'merging')

            missing = []
            for outname_index, local_outname in enumerate(self.outputs[dset]):
                base, ext = os.path.splitext(local_outname)
                remote_outname = self.outputformats[dset].format(base=base, ext=ext[1:], id=out_tag)

                handler = MergeHandler(merging_job,
                                       dset,
                                       self.__chirp,
                                       jdir,
                                       local_outname,
                                       len(self.outputs[dset]),
                                       outname_index,
                                       sdir)

                stageout = [(local_outname, os.path.join(dset, remote_outname))]
                outputs = [(os.path.join(jdir, '{0}{1}'.format(handler.base, f)), f) for f in ['cmssw.log.gz', 'report.json']]
                if not self.__chirp:
                    outputs.append((os.path.join(sdir, remote_outname), local_outname))

                inputs = self.__common_inputs[:]
                if 'X509_USER_PROXY' in os.environ:
                    inputs.append((os.environ['X509_USER_PROXY'], 'proxy'))

                for job, job_type in jobs:
                    report = self.get_report(dset, job)
                    input = resolve_name(job, job_type, local_outname, self.outputformats[dset])
                    if handler.validate(report, os.path.join(sdir, input)):
                        handler.reports.add(report)
                        handler.inputs.add(os.path.join(os.path.basename(sdir), input))
                        handler.jobs.add((job, job_type))
                        if not self.__chirp:
                            inputs.append((os.path.join(sdir, input), input))
                    else:
                        missing += [(job, job_type)]

                if len(handler.jobs) > 1:
                    args, files = handler.get_job_info(self.stageout)
                    with open(os.path.join(jdir, 'parameters.json'), 'w') as f:
                        json.dump({
                            'mask': {
                                'files': files,
                                'lumis': None
                            },
                            'monitoring': {
                                'monitorid': monitorid,
                                'syncid': syncid,
                                'taskid': self.taskid
                            },
                            'arguments': args,
                            'chirp server': self.__chirp,
                            'chirp prefix': self.stageout,
                            'transfer inputs': True,
                            'output files': stageout,
                            'want summary': True
                        }, f, indent=2)
                    inputs.append((os.path.join(jdir, 'parameters.json'), 'parameters.json'))

                    cmd = 'sh wrapper.sh python job.py merge_cfg.py parameters.json'

                    tasks.append((handler.tag, cmd, inputs, outputs))

                    self.__mergehandlers[handler.tag] = handler

                    logger.info("creating task {0} to merge {1}".format(handler.tag, resolve_joblist(sorted(jobs))))

                if len(missing) > 0:
                    template = "the following have been marked as failed because their output could not be found: {0}"
                    logger.warning(template.format(resolve_joblist(missing)))
                    self.retry(self.__store.update_missing, (missing,), {})
                    self.__missing += missing

        return tasks

    def release(self, tasks):
        jobs = defaultdict(list)
        for task in tasks:
            failed = task.return_status != 0

            handler = self.__mergehandlers[task.tag]

            if task.output:
                f = gzip.open(os.path.join(handler.jobdir, '{0}job.log.gz'.format(handler.base)), 'wb')
                f.write(task.output)
                f.close()

            files_info = {}
            files_skipped = []
            events_written = 0
            task_times = [None] * 7
            cmssw_exit_code = None
            cputime = 0
            outsize = 0

            try:
                with open(os.path.join(handler.jobdir, '{0}report.json'.format(handler.base)), 'r') as f:
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

            jobs[handler.dataset] += handler.get_job_update(failed, outsize)
            if not failed:
                try:
                    handler.merge_reports()
                    handler.cleanup()
                    self.move_jobdir(handler.id, handler.dataset, 'successful', 'merging')
                except Exception as e:
                    logger.critical('error processing {0}:\n{1}'.format(handler.id, e))
                    failed = True

            if failed:
                self.move_jobdir(handler.id, handler.dataset, 'failed', 'merging')

            logger.info("job {0} returned with exit code {1}".format(task.tag, exit_code))

            del self.__mergehandlers[task.tag]

        if len(jobs) > 0:
            self.retry(self.__store.update_merged, (jobs,), {})

    def done(self):
        done = self.__store.unfinished_merging() == 0
        if done and len(self.__missing) > 0:
            template = "the following have been marked as failed because their output could not be found: {0}"
            logger.warning(template.format(resolve_joblist(self.__missing)))

        return done

    def work_left(self):
        return self.__store.unfinished_merging()
