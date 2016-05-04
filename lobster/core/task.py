import collections
import gzip
import json
import logging
import os
import work_queue as wq

from lobster import util
from lobster.core.dataset import FileInfo
import unit

from WMCore.DataStructs.LumiList import LumiList

__all__ = ['TaskHandler', 'MergeTaskHandler', 'ProductionTaskHandler']

logger = logging.getLogger('lobster.cmssw.taskhandler')

class TaskHandler(object):
    """
    Handles mapping of lumi sections to files etc.
    """

    def __init__(self, id, dataset, files, lumis, outputs, taskdir, local=False):
        self._id = id
        self._dataset = dataset
        self._files = [(id, file) for id, file in files]
        self._file_based = any([file_ is None or run < 0 or lumi < 0 for (_, file_, run, lumi) in lumis])
        self._units = lumis
        self.outputs = outputs
        self._local = local

        self.taskdir = taskdir
        self.unit_source = 'units_' + self._dataset

    @property
    def dataset(self):
        return self._dataset

    @property
    def output_info(self):
        res = FileInfo()
        for run, lumis in self.__output_info.get('runs', {-1: [-1]}).items():
            res.lumis += [(int(run), lumi) for lumi in lumis]
        res.events = self.__output_info.get('events', 0)
        res.size = self.__output_size
        return res

    @property
    def id(self):
        return self._id

    @property
    def input_files(self):
        return list(set([filename for (id, filename) in self._files if filename]))

    def get_unit_info(self, failed, task_update, files_info, files_skipped, events_written):
        events_read = 0
        file_update = []
        unit_update = []

        units_processed = len(self._units)

        for (id, file) in self._files:
            file_units = [tpl for tpl in self._units if tpl[1] == id]

            skipped = file in files_skipped or file not in files_info
            read = 0 if failed or skipped else files_info[file][0]

            events_read += read

            if failed:
                units_processed = 0
            else:
                if skipped:
                    for (lumi_id, lumi_file, r, l) in file_units:
                        unit_update.append((unit.FAILED, lumi_id))
                        units_processed -= 1
                elif not self._file_based:
                    file_lumis = set(map(tuple, files_info[file][1]))
                    for (lumi_id, lumi_file, r, l) in file_units:
                        if (r, l) not in file_lumis:
                            unit_update.append((unit.FAILED, lumi_id))
                            units_processed -= 1

            file_update.append((read, 1 if skipped else 0, id))

        if failed:
            events_written = 0
            status = unit.FAILED
        else:
            status = unit.SUCCESSFUL

        task_update.events_read = events_read
        task_update.events_written = events_written
        task_update.units_processed = units_processed
        task_update.status = status

        return file_update, unit_update

    def adjust(self, parameters, inputs, outputs, se):
        local = self._local
        if local and se.transfer_inputs():
            inputs += [(se.local(f), os.path.basename(f), False) for id, f in self._files if f]
        if se.transfer_outputs():
            outputs += [(se.local(rf), os.path.basename(lf)) for lf, rf in self.outputs]

        parameters['mask']['files'] = self.input_files
        parameters['output files'] = self.outputs
        if not self._file_based:
            ls = LumiList(lumis=set([(run, lumi) for (id, file, run, lumi) in self._units]))
            parameters['mask']['lumis'] = ls.getCompactList()

    def process_report(self, task_update, transfers):
        """Read the report summary provided by `task.py`.
        """
        with open(os.path.join(self.taskdir, 'report.json'), 'r') as f:
            data = json.load(f)

            if len(data['files']['output info']) > 0:
                self.__output_info = data['files']['output info'].values()[0]
                self.__output_size = data['output size']

            task_update.bytes_output = data['output size']
            task_update.bytes_bare_output = data['output bare size']
            task_update.cache = data['cache']['type']
            task_update.cache_end_size = data['cache']['end size']
            task_update.cache_start_size = data['cache']['start size']
            task_update.time_wrapper_start = data['task timing']['wrapper start']
            task_update.time_wrapper_ready = data['task timing']['wrapper ready']
            task_update.time_stage_in_end = data['task timing']['stage in end']
            task_update.time_prologue_end = data['task timing']['prologue end']
            task_update.time_processing_end = data['task timing']['processing end']
            task_update.time_epilogue_end = data['task timing']['epilogue end']
            task_update.time_stage_out_end = data['task timing']['stage out end']
            task_update.time_cpu = data['cpu time']

            files_info = data['files']['info']
            files_skipped = data['files']['skipped']
            events_written = data['events written']
            exe_exit_code = data['exe exit code']
            stageout_exit_code = data['stageout exit code']
            task_exit_code = data['task exit code']

            for protocol in data['transfers']:
                transfers[self._dataset][protocol] += collections.Counter(data['transfers'][protocol])

            return files_info, files_skipped, events_written, exe_exit_code, stageout_exit_code, task_exit_code

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
        if task.resources_requested:
            task_update.requested_cores = task.resources_requested.cores
            task_update.requested_disk = task.resources_requested.disk
            task_update.requested_memory = task.resources_requested.memory
        if task.resources_measured:
            task_update.cores = task.resources_measured.cores
            task_update.workdir_num_files = task.resources_measured.total_files
            task_update.workdir_footprint = task.resources_measured.disk
            task_update.memory_resident = task.resources_measured.memory
            task_update.memory_swap = task.resources_measured.swap_memory
            task_update.memory_virtual = task.resources_measured.virtual_memory
            task_update.network_bandwidth = task.resources_measured.bandwidth
            task_update.network_bytes_received = task.resources_measured.bytes_received
            task_update.network_bytes_sent = task.resources_measured.bytes_sent

    def process(self, task, summary, transfers):
        exit_code = task.return_status
        failed = (exit_code != 0)

        task_update = unit.TaskUpdate()

        # Save wrapper output
        if task.output:
            f = gzip.open(os.path.join(self.taskdir, 'task.log.gz'), 'wb')
            f.write(task.output)
            f.close()

        # CMS stats to update
        files_info = {}
        files_skipped = []
        exe_exit_code = None
        stageout_exit_code = None
        task_exit_code = None
        events_written = 0

        # May not all be there for failed tasks
        try:
            files_info, files_skipped, events_written, exe_exit_code, stageout_exit_code, task_exit_code = self.process_report(task_update, transfers)
        except (ValueError, EOFError) as e:
            failed = True
            logger.error("error processing {0}:\n{1}".format(task.tag, e))
        except IOError as e:
            failed = True
            logger.error("error processing {1} from {0}".format(task.tag, os.path.basename(e.filename)))

        # Determine true status
        if task.result != wq.WORK_QUEUE_RESULT_SUCCESS and task.result != wq.WORK_QUEUE_RESULT_OUTPUT_MISSING:
            failed = True
            summary.wq(task.result, task.tag)

            if task.result == wq.WORK_QUEUE_RESULT_MAX_RETRIES:
                exit_code = 10020
            elif task.result == wq.WORK_QUEUE_RESULT_TASK_MAX_RUN_TIME:
                exit_code = 10030
            elif task.result == wq.WORK_QUEUE_RESULT_TASK_TIMEOUT:
                exit_code = 10010
            elif task.result == wq.WORK_QUEUE_RESULT_RESOURCE_EXHAUSTION:
                if task.resources_measured.limits_exceeded.wall_time > 0:
                    exit_code = 10030
                elif task.resources_measured.limits_exceeded.memory > 0:
                    exit_code = 10040
                elif task.resources_measured.limits_exceeded.disk > 0:
                    exit_code = 10050
            else:
                exit_code = 10001
        # If the executable failed, everything else is going to fail.
        # If stage-out fails, the task is going to fail.  If neither
        # has happened, something else has gone wrong.
        elif exe_exit_code not in (None, 0):
            exit_code = exe_exit_code
            failed = True
            summary.exe(exit_code, task.tag)
        elif stageout_exit_code not in (None, 0):
            exit_code = stageout_exit_code
            failed = True
            summary.exe(exit_code, task.tag)
        elif task_exit_code not in (None, 0):
            exit_code = task_exit_code
            failed = True
            summary.exe(exit_code, task.tag)
        # Catch remaining tasks that somehow failed, but we could find
        # nothing wrong with them, yet WQ tells us output is missing.
        elif task.result != wq.WORK_QUEUE_RESULT_SUCCESS:
            exit_code = 10001
            failed = True
            summary.wq(task.result, task.tag)
        else:
            summary.exe(exit_code, task.tag)

        task_update.exit_code = exit_code

        # Update CMS stats
        file_update, unit_update = self.get_unit_info(failed, task_update, files_info, files_skipped, events_written)
        try:
            self.process_wq_info(task, task_update)
        except AttributeError as e:
            logger.debug('Error processing WQ info:{}'.format(e))
            summary.monitor(task.tag)

        return failed, task_update, file_update, unit_update


class MergeTaskHandler(TaskHandler):
    def __init__(self, id_, dataset, files, lumis, outputs, taskdir):
        super(MergeTaskHandler, self).__init__(id_, dataset, files, lumis, outputs, taskdir)
        self._local = True
        self._file_based = True
        self.unit_source = 'tasks'

    def get_unit_info(self, failed, task_update, files_info, files_skipped, events_written):
        _, up = super(MergeTaskHandler, self).get_unit_info(failed, task_update, files_info, files_skipped, events_written)
        return [], up

class ProductionTaskHandler(TaskHandler):
    def __init__(self, id_, dataset, lumis, outputs, taskdir):
        super(ProductionTaskHandler, self).__init__(id_, dataset, [], lumis, outputs, taskdir)
        self._file_based = True

    def adjust(self, parameters, inputs, outputs, se):
        super(ProductionTaskHandler, self).adjust(parameters, inputs, outputs, se)
        parameters['mask']['first lumi'] = self._units[0][3]

    def get_unit_info(self, failed, task_update, files_info, files_skipped, events_written):
        _, up = super(ProductionTaskHandler, self).get_unit_info(failed, task_update, files_info, files_skipped, events_written)
        return [(0, 0, 1)], up
