import datetime
# import getpass
# import pwd
import os
import subprocess

from hashlib import sha1

from DashboardAPI import apmonSend, apmonFree
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from lobster import util

import DashboardAPI
DashboardAPI.apmonLoggingLevel = "DEBUG"
import time
import work_queue as wq

UNKNOWN = 'Unknown'
SUBMITTED = 'Pending'
DONE = 'Done'
RETRIEVED = 'Retrieved'
ABORTED = 'Aborted'
CANCELLED = 'Killed'
RUNNING = 'Running'
WAITING_RETRIEVAL = 'Waiting Retrieval'

# dictionary between work queue and dashboard status
status_map = {
    wq.WORK_QUEUE_TASK_UNKNOWN: UNKNOWN,
    wq.WORK_QUEUE_TASK_READY: SUBMITTED,
    wq.WORK_QUEUE_TASK_RUNNING: RUNNING,
    wq.WORK_QUEUE_TASK_WAITING_RETRIEVAL: WAITING_RETRIEVAL,
    wq.WORK_QUEUE_TASK_RETRIEVED: RETRIEVED,
    wq.WORK_QUEUE_TASK_DONE: DONE,
    wq.WORK_QUEUE_TASK_CANCELED: ABORTED
}


class DummyMonitor(object):
    def __init__(self, workdir):
        self._taskid = util.checkpoint(workdir, 'id')

    def generate_ids(self, jobid):
        monitorid = '{0}_{1}/{0}'.format(jobid, 'https://ndcms.crc.nd.edu/{0}'.format(sha1(self._taskid).hexdigest()[-16:]))
        syncid = 'https://ndcms.crc.nd.edu//{0}//12345.{1}'.format(self._taskid, jobid)

        return monitorid, syncid

    def register_run(self):
        pass

    def register_job(self, id):
        """Returns Dashboard MonitorJobID and SyncId."""
        return None, None

    def update_job(self, id, status):
        pass

    def free(self):
        pass

class Monitor(DummyMonitor):
    def __init__(self, workdir):
        super(Monitor, self).__init__(workdir)

        p = subprocess.Popen(["voms-proxy-info", "-identity"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        id, err = p.communicate()
        id = id.strip()
        db = SiteDBJSON({'cacheduration': 24})

        self.__username = db.dnUserName(dn=id)
        self.__fullname = id.rsplit('/CN=', 1)[1]
        # self.__fullname = pwd.getpwnam(getpass.getuser())[4]
        if util.checkpoint(workdir, "sandbox cmssw version"):
            self.__cmssw_version = str(util.checkpoint(workdir, "sandbox cmssw version"))
        else:
            self.__cmssw_version = 'Unknown'
        if util.checkpoint(workdir, "executable"):
            self.__executable = str(util.checkpoint(workdir, "executable"))
        else:
            self.__executable = 'Unknown'

    def __del__(self):
        self.free()

    def free(self):
        apmonFree()

    def register_run(self):
        apmonSend(self._taskid, 'TaskMeta', {
            'taskId': self._taskid,
            'jobId': 'TaskMeta',
            'tool': 'lobster',
            'tool_ui': os.environ.get('HOSTNAME',''),
            'SubmissionType': 'direct',
            'JSToolVersion': '3.2.1',
            'scheduler': 'work_queue',
            'GridName': '/CN=' + self.__fullname,
            'ApplicationVersion': self.__cmssw_version,
            'taskType': 'analysis',
            'vo': 'cms',
            'CMSUser': self.__username,
            'user': self.__username,
            'datasetFull': '',
            'resubmitter': 'user',
            'exe': self.__executable
            })
        self.free()

    def register_job(self, id):
        monitorid, syncid = self.generate_ids(id)
        apmonSend(self._taskid, monitorid, {
            'taskId': self._taskid,
            'jobId': monitorid,
            'sid': syncid,
            'broker': 'condor',
            'bossId': str(id),
            'SubmissionType': 'Direct',
            'TargetSE': 'ndcms.crc.nd.edu',
            'localId' : '',
            'tool': 'lobster',
            'JSToolVersion': '3.2.1',
            'tool_ui': os.environ.get('HOSTNAME',''),
            'scheduler': 'work_queue',
            'GridName': '/CN=' + self.__fullname,
            'ApplicationVersion': self.__cmssw_version,
            'taskType': 'analysis',
            'vo': 'cms',
            'CMSUser': self.__username,
            'user': self.__username,
            # 'datasetFull': self.datasetPath,
            'resubmitter': 'user',
            'exe': self.__executable
            })
        return monitorid, syncid

    def update_job(self, id, status):
        monitorid, syncid = self.generate_ids(id)
        apmonSend(self._taskid, monitorid, {
            'taskId': self._taskid,
            'jobId': monitorid,
            'sid': syncid,
            'StatusValueReason': '',
            'StatusValue': status,
            'StatusEnterTime':
            "{0:%F_%T}".format(datetime.datetime.utcnow()),
            'StatusDestination': 'ndcms.crc.nd.edu',
            'RBname': 'condor'
            })


class JobStateChecker(object):
    """
    Check the job state  at a given time interval
    """

    def __init__(self, interval):
        self._t_interval = interval
        self._t_previous = 0
        self._previous_states = {}

    def report_in_interval(self, t_current):
        """
        Returns True if the lapse time between the current time
        and the last time reported is greater than the interval time defined
        """

        report = t_current - self._t_previous >= self._t_interval \
            if self._t_previous else True
        return report

    def update_dashboard_states(self, monitor, queue, exclude_states):
        """
        Update dashboard states for all jobs.
        This is done only if the job status changed.
        """
        t_current = time.time()
        if self.report_in_interval(t_current):
            self._t_previous = t_current
            try:
                ids_list = queue._task_table.keys()
            except:
                raise

            for id in ids_list:
                status = status_map[queue.task_state(id)]
                status_new_or_changed = not self._previous_states.get(id) or \
                    self._previous_states.get(id, status) != status

                if status not in exclude_states and status_new_or_changed:
                    try:
                        monitor.update_job(id, status)
                    except:
                        raise

                if status_new_or_changed:
                    self._previous_states.update({id: status})
