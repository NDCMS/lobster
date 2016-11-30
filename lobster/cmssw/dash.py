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

logger = logging.getLogger('lobster.cmssw.dashboard')

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


class Monitor(object):

    def setup(self, config):
        self._workflowid = util.checkpoint(config.workdir, 'id')

    def generate_ids(self, taskid):
        return "dummy", "dummy"

    def register_run(self):
        pass

    def register_task(self, id):
        """Returns Dashboard MonitorJobID and SyncId."""
        return None, None

    def update_task(self, id, status):
        pass

    def update_tasks(self, queue, exclude):
        pass

    def free(self):
        pass


class Dashboard(Monitor, util.Configurable):

    _mutable = {}

    def __init__(self, interval=300, username=None, fullname=None):
        self.interval = interval
        self.__previous = 0
        self.__states = {}
        self.username = username if username else self.__get_user()
        self.fullname = fullname if fullname else self.__get_distinguished_name().rsplit('/CN=', 1)[1]

        self.__cmssw_version = 'Unknown'
        self.__executable = 'Unknown'

        try:
            self._ce = loadSiteLocalConfig().siteName
        except SiteConfigError:
            logger.error("can't load siteconfig, defaulting to hostname")
            self._ce = socket.getfqdn()

    def __del__(self):
        try:
            self.free()
        except Exception:
            pass

    def __get_distinguished_name(self):
        p = subprocess.Popen(["voms-proxy-info", "-identity"],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
        id_, err = p.communicate()
        return id_.strip()

    def __get_user(self):
        db = SiteDBJSON({'cacheduration': 24, 'logger': logging.getLogger("WMCore")})
        return db.dnUserName(dn=self.__get_distinguished_name())

    def free(self):
        apmonFree()

    def send(self, taskid, params):
        apmonSend(self._workflowid, taskid, params, logging, conf)

    def setup(self, config):
        super(Dashboard, self).setup(config)
        if util.checkpoint(config.workdir, "sandbox cmssw version"):
            self.__cmssw_version = str(util.checkpoint(config.workdir, "sandbox cmssw version"))
        if util.checkpoint(config.workdir, "executable"):
            self.__executable = str(util.checkpoint(config.workdir, "executable"))

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
            'GridName': '/CN=' + self.fullname,
            'ApplicationVersion': self.__cmssw_version,
            'taskType': 'analysis',
            'vo': 'cms',
            'CMSUser': self.username,
            'user': self.username,
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
            'GridName': '/CN=' + self.fullname,
            'ApplicationVersion': self.__cmssw_version,
            'taskType': 'analysis',
            'vo': 'cms',
            'CMSUser': self.username,
            'user': self.username,
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

    def update_tasks(self, queue, exclude):
        """
        Update dashboard states for all tasks.
        This is done only if the task status changed.
        """
        report = time.time() > self.__previous + self.interval
        if not report:
            return
        with util.PartiallyMutable.unlock():
            self.__previous = time.time()

        try:
            ids = queue._task_table.keys()
        except Exception:
            raise

        for id_ in ids:
            status = status_map[queue.task_state(id_)]
            if status in exclude:
                continue
            if not self.__states.get(id_) or self.__states.get(id_, status) != status:
                continue

            self.update_task(id_, status)
            self.__states.update({id_: status})
