import gzip
import json
import logging
import os
import work_queue as wq

from lobster.cmssw import jobit

from WMCore.DataStructs.LumiList import LumiList

logger = logging.getLogger('lobster.cmssw.taskhandler')

class TaskHandler(object):
    """
    Handles mapping of lumi sections to files etc.
    """

    def __init__(
            self, id, dataset, files, lumis, outputs, jobdir,
            cmssw_job=True, empty_source=False, merge=False, local=False):
        self._id = id
        self._dataset = dataset
        self._files = [(id, file) for id, file in files]
        self._file_based = any([run < 0 or lumi < 0 for (id, file, run, lumi) in lumis])
        self._jobits = lumis
        self.jobdir = jobdir
        self._outputs = outputs
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
    def outputs(self):
        return self._outputs

    @property
    def id(self):
        return self._id

    @property
    def input_files(self):
        return list(set([filename for (id, filename) in self._files if filename]))

    @property
    def jobit_source(self):
        return 'jobs' if self._merge else 'jobits_' + self._dataset

    @property
    def merge(self):
        return self._merge

    def get_jobit_info(self, failed, task_update, files_info, files_skipped, events_written):
        events_read = 0
        file_update = []
        jobit_update = []

        jobits_processed = len(self._jobits)

        for (id, file) in self._files:
            file_jobits = [tpl for tpl in self._jobits if tpl[1] == id]

            skipped = False
            read = 0
            if self._cmssw_job:
                if not self._empty_source:
                    skipped = file in files_skipped or file not in files_info
                    read = 0 if failed or skipped else files_info[file][0]

            events_read += read

            if failed:
                jobits_processed = 0
            else:
                if skipped:
                    for (lumi_id, lumi_file, r, l) in file_jobits:
                        jobit_update.append((jobit.FAILED, lumi_id))
                        jobits_processed -= 1
                elif not self._file_based:
                    file_lumis = set(map(tuple, files_info[file][1]))
                    for (lumi_id, lumi_file, r, l) in file_jobits:
                        if (r, l) not in file_lumis:
                            jobit_update.append((jobit.FAILED, lumi_id))
                            jobits_processed -= 1

            file_update.append((read, 1 if skipped else 0, id))

        if failed:
            events_written = 0
            status = jobit.FAILED
        else:
            status = jobit.SUCCESSFUL

        if self._merge:
            file_update = []
            # FIXME not correct
            jobits_missed = 0

        task_update.events_read = events_read
        task_update.events_written = events_written
        task_update.jobits_processed = jobits_processed
        task_update.status = status

        return file_update, jobit_update

    def adjust(self, parameters, inputs, outputs, se):
        local = self._local or self._merge
        if local and se.transfer_inputs():
            inputs += [(se.local(f), os.path.basename(f), False) for id, f in self._files if f]
        if se.transfer_outputs():
            outputs += [(se.local(rf), os.path.basename(lf)) for lf, rf in self._outputs]

        parameters['mask']['files'] = self.input_files
        parameters['output files'] = self._outputs
        if not self._file_based and not self._merge:
            ls = LumiList(lumis=set([(run, lumi) for (id, file, run, lumi) in self._jobits]))
            parameters['mask']['lumis'] = ls.getCompactList()

    def process_report(self, task_update):
        """Read the report summary provided by `job.py`.
        """
        with open(os.path.join(self.jobdir, 'report.json'), 'r') as f:
            data = json.load(f)
            task_update.bytes_output = data['output size']
            task_update.bytes_bare_output = data['output bare size']
            task_update.cache = data['cache']['type']
            task_update.cache_end_size = data['cache']['end size']
            task_update.cache_start_size = data['cache']['start size']
            task_update.time_wrapper_start = data['task timing']['time wrapper start']
            task_update.time_wrapper_ready = data['task timing']['time wrapper ready']
            task_update.time_stage_in_end = data['task timing']['time stage in end']
            task_update.time_prologue_end = data['task timing']['time prologue end']
            task_update.time_file_requested = data['task timing']['time file requested']
            task_update.time_file_opened = data['task timing']['time file opened']
            task_update.time_file_processing = data['task timing']['time file processing']
            task_update.time_processing_end = data['task timing']['time processing end']
            task_update.time_epilogue_end = data['task timing']['time epilogue end']
            task_update.time_stage_out_end = data['task timing']['time stage out end']
            task_update.time_cpu = data['cpu time']
            if self._cmssw_job:
                files_info = data['files']['info']
                files_skipped = data['files']['skipped']
                events_written = data['events written']
                cmssw_exit_code = data['cmssw exit code']
                return files_info, files_skipped, events_written, cmssw_exit_code
            return {}, [], 0, None

    def process_wq_info(self, task, task_update):
        """Extract useful information from the Work Queue task object.
        """
        task_update.host = util.verify_string(task.hostname)
        task_update.id = task.tag
        task_update.submissions = task.total_submissions
        task_update.bytes_received = task.total_bytes_received
        task_update.bytes_sent = task.total_bytes_sent
        task_update.time_submit = task.submit_time / 1000000
        task_update.time_transfer_in_start = task.send_input_start / 1000000
        task_update.time_transfer_in_end = task.send_input_finish / 1000000
        task_update.time_transfer_out_start = task.receive_output_start / 1000000
        task_update.time_transfer_out_end = task.receive_output_finish / 1000000
        task_update.time_retrieved = task.finish_time / 1000000
        task_update.time_on_worker = task.cmd_execution_time / 1000000
        task_update.time_total_on_worker = task.total_cmd_execution_time / 1000000
        task_update.workdir_num_files = task.resources_measured.workdir_num_files
        task_update.workdir_footprint = task.resources_measured.workdir_footprint
        task_update.limits_exceeded = task.resources_measured.limits_exceeded
        task_update.memory_resident = task.resources_measured.resident_memory
        task_update.memory_swap = task.resources_measured.swap_memory
        task_update.memory_virtual = task.resources_measured.virtual_memory

    def process(self, task, summary):
        exit_code = task.return_status
        failed = (exit_code != 0)

        task_update = jobit.JobUpdate()

        # Save wrapper output
        if task.output:
            f = gzip.open(os.path.join(self.jobdir, 'job.log.gz'), 'wb')
            f.write(task.output)
            f.close()

        # CMS stats to update
        files_info = {}
        files_skipped = []
        cmssw_exit_code = None
        events_written = 0

        # May not all be there for failed jobs
        try:
            files_info, files_skipped, events_written, cmssw_exit_code = self.process_report(task_update)
        except (ValueError, EOFError) as e:
            failed = True
            logger.error("error processing {0}:\n{1}".format(task.tag, e))
        except IOError as e:
            failed = True
            logger.error("error processing {1} from {0}".format(task.tag, os.path.basename(e.filename)))

        # Determine true status
        if not exit_code and task.result != wq.WORK_QUEUE_RESULT_SUCCESS:
            exit_code = 100000 + task.result
            failed = True
            summary.wq(task.result, task.tag)
        else:
            if cmssw_exit_code not in (None, 0):
                exit_code = cmssw_exit_code
                if exit_code > 0:
                    failed = True
            summary.exe(exit_code, task.tag)
        task_update.exit_code = exit_code

        # Update CMS stats
        file_update, jobit_update = self.get_jobit_info(failed, task_update, files_info, files_skipped, events_written)
        try:
            self.process_wq_info(task, task_update)
        except AttributeError:
            summary.monitor(task.tag)

        return failed, task_update, file_update, jobit_update
