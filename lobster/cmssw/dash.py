import datetime
import logging
import os
import socket
import subprocess

from hashlib import sha1

from WMCore.Services.Dashboard.DashboardAPI import apmonSend, apmonFree
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig, SiteConfigError
from lobster import util

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

conf = {
    'cms-jobmon.cern.ch:8884': {
        'sys_monitoring': 0,
        'general_info': 0,
        'job_monitoring': 0
    }
}


class DummyMonitor(object):

    def __init__(self, workdir):
        self._workflowid = util.checkpoint(workdir, 'id')

    def generate_ids(self, taskid):
        return "dummy", "dummy"

    def register_run(self):
        pass

    def register_task(self, id):
        """Returns Dashboard MonitorJobID and SyncId."""
        return None, None

    def update_task(self, id, status):
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
            self.__cmssw_version = str(util.checkpoint(
                workdir, "sandbox cmssw version"))
        else:
            self.__cmssw_version = 'Unknown'
        if util.checkpoint(workdir, "executable"):
            self.__executable = str(util.checkpoint(workdir, "executable"))
        else:
            self.__executable = 'Unknown'

        try:
            self._ce = loadSiteLocalConfig().siteName
        except SiteConfigError:
            logger.error("can't load siteconfig, defaulting to hostname")
            self._ce = socket.getfqdn()

    def __del__(self):
        self.free()

    def free(self):
        apmonFree()

    def send(self, taskid, params):
        apmonSend(self._workflowid, taskid, params, logging, conf)

    def generate_ids(self, taskid):
        seid = 'https://{}/{}'.format(self._ce,
                                      sha1(self._workflowid).hexdigest()[-16:])
        monitorid = '{0}_{1}/{0}'.format(taskid, seid)
        syncid = 'https://{}//{}//12345.{}'.format(
            self._ce, self._workflowid, taskid)

        return monitorid, syncid

    def register_run(self):
        self.send('TaskMeta', {
            'taskId': self._workflowid,
            'jobId': 'TaskMeta',
            'tool': 'lobster',
            'tool_ui': os.environ.get('HOSTNAME', ''),
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

    def register_task(self, id):
        monitorid, syncid = self.generate_ids(id)
        self.send(monitorid, {
            'taskId': self._workflowid,
            'jobId': monitorid,
            'sid': syncid,
            'broker': 'condor',
            'bossId': str(id),
            'SubmissionType': 'Direct',
            'TargetSE': 'Many_Sites',  # XXX This should be the SE where input data is stored
            'localId': '',
            'tool': 'lobster',
            'JSToolVersion': '3.2.1',
            'tool_ui': os.environ.get('HOSTNAME', ''),
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

    def update_task(self, id, status):
        monitorid, syncid = self.generate_ids(id)
        self.send(monitorid, {
            'taskId': self._workflowid,
            'jobId': monitorid,
            'sid': syncid,
            'StatusValueReason': '',
            'StatusValue': status,
            'StatusEnterTime':
            "{0:%F_%T}".format(datetime.datetime.utcnow()),
            # Destination will be updated by the task once it sends a dashboard update.
            # in line with
            # https://github.com/dmwm/WMCore/blob/6f3570a741779d209f0f720647642d51b64845da/src/python/WMCore/Services/Dashboard/DashboardReporter.py#L136
            'StatusDestination': 'Unknown',
            'RBname': 'condor'
        })


class TaskStateChecker(object):
    """
    Check the task state  at a given time interval
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
        Update dashboard states for all tasks.
        This is done only if the task status changed.
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
                        monitor.update_task(id, status)
                    except:
                        raise

                if status_new_or_changed:
                    self._previous_states.update({id: status})
