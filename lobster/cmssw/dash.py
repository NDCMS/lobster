import datetime
# import getpass
# import pwd
import os
import subprocess

from hashlib import sha1

from DashboardAPI import apmonSend, apmonFree
from WMCore.Services.SiteDB.SiteDB import SiteDBJSON

SUBMITTED = 'Pending'
DONE = 'Done'
ABORTED = 'Aborted'

class DummyMonitor(object):
    def __init__(self, taskid):
        self._taskid = taskid

    def generate_ids(self, jobid):
        syncid = 'https://ndcms.crc.nd.edu/{1}/{0}'.format(
                jobid,
                sha1(self._taskid).hexdigest()[-16:])
        monitorid = '{0}_{1}'.format(jobid, syncid)
        sid = 'https://ndcms.crc.nd.edu//{0}//12345.{1}'.format(self._taskid, jobid)

        return monitorid, syncid, sid

    def register_run(self):
        pass

    def register_job(self, id):
        """Returns Dashboard MonitorJobID and SyncGridJobId."""
        return None, None

    def update_job(self):
        pass

    def free(self):
        pass

class Monitor(DummyMonitor):
    def __init__(self, taskid):
        super(Monitor, self).__init__(taskid)

        p = subprocess.Popen(["voms-proxy-info", "-identity"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        id, err = p.communicate()
        id = id.strip()
        db = SiteDBJSON({'cacheduration': 24})

        self.__username = db.dnUserName(dn=id)
        self.__fullname = id.rsplit('/CN=', 1)[1]
        # self.__fullname = pwd.getpwnam(getpass.getuser())[4]

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
            'ApplicationVersion': os.path.basename(os.path.normpath(os.environ.get('LOCALRT'))),
            'taskType': 'analysis',
            'vo': 'cms',
            'CMSUser': self.__username,
            'user': self.__username,
            'datasetFull': '',
            'resubmitter': 'user',
            'exe': 'cmsRun'
            })
        self.free()

    def register_job(self, id):
        monitorid, syncid, sid = self.generate_ids(id)
        apmonSend(self._taskid, monitorid, {
            'taskId': self._taskid,
            'jobId': monitorid,
            'sid': sid,
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
            'ApplicationVersion': os.path.basename(os.path.normpath(os.environ.get('LOCALRT'))),
            'taskType': 'analysis',
            'vo': 'cms',
            'CMSUser': self.__username,
            'user': self.__username,
            # 'datasetFull': self.datasetPath,
            'resubmitter': 'user',
            'exe': 'cmsRun'
            })
        self.update_job(id, SUBMITTED)
        return monitorid, syncid

    def update_job(self, id, status):
        monitorid, syncid, sid = self.generate_ids(id)
        apmonSend(self._taskid, monitorid, {
            'taskId': self._taskid,
            'jobId': monitorid,
            'sid': sid,
            'StatusValueReason': '',
            'StatusValue': status,
            'StatusEnterTime':
            "{0:%F_%T}".format(datetime.datetime.utcnow()),
            'StatusDestination': 'ndcms.crc.nd.edu',
            'RBname': 'condor'
            })
