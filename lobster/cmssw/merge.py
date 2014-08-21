from collections import defaultdict
import os
import subprocess
import yaml
import multiprocessing
import gzip
import jobit
import pickle
import logging
import shutil

from IMProv.IMProvDoc import IMProvDoc
from ProdCommon.FwkJobRep.ReportParser import readJobReport

from lobster import job, util
import jobit
import dash

def resolve_path(workdir, job, merge_job):
    topdir = 'successful' if merge_job == 0 else 'merged'
    bottomdir = str(job) if merge_job == 0 else str(merge_job)

    return os.path.join(workdir, topdir, bottomdir, 'report.xml.gz')

def resolve_name(job, merge_job, name, name_format):
    base, ext = os.path.splitext(name)
    id = str(job) if merge_job == 0 else 'merged_{0}'.format(merge_job)

    return name_format.format(base=base, ext=ext[1:], id=id)

class MergeHandler(object):
    def __init__(self, id, chirp, jobdir, outname, num_outputs, outname_index, sdir, jobs):
        self.__id = id
        self.__chirp = chirp
        self.__jobdir = jobdir
        self.__outname = outname
        self.__sdir = sdir
        self.__jobs = jobs

        self.__reports = []
        self.__inputs = []

        if num_outputs == 1:
            self.__tag = str(id)
            self.__base = ''
        else:
            base, ext = os.path.splitext(outname)
            self.__tag = '{0}_{1}'.format(id, outname_index)
            self.__base = '{0}_'.format(base)

    @property
    def jobs(self):
        return self.__jobs

    @property
    def reports(self):
        return self.__reports

    @property
    def inputs(self):
        return self.__inputs

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
        if failed:
            status = jobit.FAILED
        else:
            status = jobit.SUCCESSFUL

        return [(job, self.__id, status, outsize) for job in self.__jobs]

    def get_job_info(self):
        args = ['output=' + self.__outname]
        if self.__chirp:
            args += ['chirp={0}'.format(self.__chirp)]
            args += [str('inputs=' + ','.join(self.__inputs))]

        files = ['file:' + os.path.basename(x) for x in self.__inputs]

        return args, files

    def merge_reports(self):
        reports = []
        for r in self.__reports:
            f = gzip.open(r)
            report = readJobReport(f)
            reports.extend(report)
            f.close()

        output = IMProvDoc("JobReports")
        for item in reports:
            output.addNode(item.save())

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
        self.__mergehandlers = {}

        self.__store = jobit.JobitStore(self.config)
        self.__store.reset_merging()
        logging.info("registering unmerged jobs")
        self.__store.register_unmerged()

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

    def obtain(self, num=1):
        unmerged_jobs = self.retry(self.__store.pop_unmerged_jobs, (num,), {})
        if not unmerged_jobs or len(unmerged_jobs) == 0:
            return None

        tasks = []
        for merging_job, dset, jobs in unmerged_jobs:
            out_tag = 'merged_{0}'.format(merging_job)

            monitorid, syncid = self.__dash.register_job(merging_job)

            sdir = os.path.join(self.stageout, dset)
            jdir = os.path.join(self.workdir, dset, 'merging', str(merging_job))
            if not os.path.isdir(jdir):
                os.makedirs(jdir)

            for outname_index, local_outname in enumerate(self.outputs[dset]):
                base, ext = os.path.splitext(local_outname)
                remote_outname = self.outputformats[dset].format(base=base, ext=ext[1:], id=out_tag)

                handler = MergeHandler(merging_job,
                                       self.__chirp,
                                       jdir,
                                       local_outname,
                                       len(self.outputs[dset]),
                                       outname_index,
                                       sdir,
                                       [job for job, merged_job in jobs])

                stageout = [(local_outname, os.path.join(dset, remote_outname))]
                outputs = [(os.path.join(jdir, '{0}{1}'.format(handler.base, f)), f) for f in ['cmssw.log.gz', 'report.pkl']]
                if not self.__chirp:
                    outputs.append((os.path.join(sdir, remote_outname), local_outname))

                inputs = self.__common_inputs[:]
                if 'X509_USER_PROXY' in os.environ:
                    inputs.append((os.environ['X509_USER_PROXY'], 'proxy'))

                for job, merged_job in jobs:
                    handler.reports.append(resolve_path(os.path.join(self.workdir, dset), job, merged_job))

                    input = resolve_name(job, merged_job, local_outname, self.outputformats[dset])
                    handler.inputs.append(os.path.join(os.path.basename(sdir), input))

                    if not self.__chirp:
                        inputs.append((os.path.join(sdir, input), input))

                args, files = handler.get_job_info()
                with open(os.path.join(jdir, 'parameters.pkl'), 'wb') as f:
                    pickle.dump((args, files, None, stageout, self.__chirp, self.taskid, monitorid, syncid, True), f, pickle.HIGHEST_PROTOCOL)
                inputs.append((os.path.join(jdir, 'parameters.pkl'), 'parameters.pkl'))

                cmd = 'sh wrapper.sh python job.py merge_cfg.py parameters.pkl'

                tasks.append((handler.tag, cmd, inputs, outputs))

                self.__mergehandlers[handler.tag] = handler

                resolved_ids = [str(job) if merged_job==0 else str(merged_job) for job, merged_job in jobs]
                logging.info("creating task {0} to merge jobs {1}".format(handler.tag, ", ".join(resolved_ids)))

        return tasks

    def release(self, tasks):
        jobs = []
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
                with open(os.path.join(handler.jobdir, '{0}report.pkl'.format(handler.base)), 'rb') as f:
                    files_info, files_skipped, events_written, task_times, cmssw_exit_code, cputime, outsize = pickle.load(f)
            except (EOFError, IOError) as e:
                failed = True
                logging.error("error processing {0}:\n{1}".format(task.tag, e))

            if cmssw_exit_code not in (None, 0):
                exit_code = cmssw_exit_code
                if exit_code > 0:
                    failed = True
            else:
                exit_code = task.return_status

            jobs += handler.get_job_update(failed, outsize)
            if failed:
                shutil.move(handler.jobdir, handler.jobdir.replace('merging', 'merge_failed'))
            else:
                handler.merge_reports()
                handler.cleanup()
                shutil.move(handler.jobdir, handler.jobdir.replace('merging', 'merged'))

            logging.info("job {0} returned with exit code {1}".format(task.tag, exit_code))

            del self.__mergehandlers[task.tag]

        if len(jobs) > 0:
            self.retry(self.__store.update_merged, (jobs,), {})

    def done(self):
        return self.__store.unfinished_merging() == 0

    def work_left(self):
        return self.__store.unfinished_merging()
