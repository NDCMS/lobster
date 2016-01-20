import math
import os
import re
import requests
from retrying import retry
import shutil
import tempfile

from lobster.core.dataset import FileInfo, DatasetInfo
from lobster.util import Configurable

from dbs.apis.dbsClient import DbsApi
from WMCore.DataStructs.LumiList import LumiList


class DASWrapper(DbsApi):
    @retry(stop_max_attempt_number=10)
    def listFileLumis(self, *args, **kwargs):
        return super(DASWrapper, self).listFileLumis(*args, **kwargs)

    @retry(stop_max_attempt_number=10)
    def listFileSummaries(self, *args, **kwargs):
        return super(DASWrapper, self).listFileSummaries(*args, **kwargs)

    @retry(stop_max_attempt_number=10)
    def listFiles(self, *args, **kwargs):
        return super(DASWrapper, self).listFiles(*args, **kwargs)

    @retry(stop_max_attempt_number=10)
    def listBlocks(self, *args, **kwargs):
        return super(DASWrapper, self).listBlocks(*args, **kwargs)


class Cache(object):
    def __init__(self):
        self.cache = tempfile.mkdtemp()
    def __del__(self):
        shutil.rmtree(self.cache)

class Dataset(Configurable):
    _mutable = []

    __apis = {}
    __dsets = {}
    __cache = Cache()

    def __init__(self, dataset, lumis_per_task=25, events_per_task=None, lumi_mask=None, file_based=False, dbs_instance='global'):
        self.dataset = dataset
        self.lumi_mask = lumi_mask
        self.lumis_per_task = lumis_per_task
        self.events_per_task = events_per_task
        self.file_based = file_based
        self.dbs_instance = dbs_instance

        self.total_units = 0

    def __get_mask(self, url):
        if not re.match(r'https?://', url):
            return url

        fn = os.path.basename(url)
        cached = os.path.join(Dataset.__cache.cache, fn)
        if not os.path.isfile(cached):
            r = requests.get(url)
            if not r.ok:
                raise IOError("unable to retrieve '{0}'".format(url))
            with open(cached, 'w') as f:
                f.write(r.text)
        return cached

    def get_info(self):
        if self.dataset not in Dataset.__dsets:
            if self.lumi_mask:
                self.lumi_mask = self.__get_mask(self.lumi_mask)
            res = self.query_database(self.dataset, self.dbs_instance, self.lumi_mask, self.file_based)

            if self.events_per_task:
                res.tasksize = int(math.ceil(self.events_per_task / float(res.total_events) * res.total_lumis))
            else:
                res.tasksize = self.lumis_per_task

            Dataset.__dsets[self.dataset] = res

        self.total_units = Dataset.__dsets[self.dataset].total_lumis
        return Dataset.__dsets[self.dataset]

    def query_database(self, dataset, instance, mask, file_based):
        if instance not in self.__apis:
            dbs_url = 'https://cmsweb.cern.ch/dbs/prod/{0}/DBSReader'.format(instance)
            self.__apis[instance] = DASWrapper(dbs_url)

        result = DatasetInfo()

        infos = self.__apis[instance].listFileSummaries(dataset=dataset)
        result.total_events = sum([info['num_event'] for info in infos])
        result.unmasked_lumis = sum([info['num_lumi'] for info in infos])

        for info in self.__apis[instance].listFiles(dataset=dataset, detail=True):
            fn = info['logical_file_name']
            result.files[fn].events = info['event_count']
            result.files[fn].size = info['file_size']

        files = set()
        if file_based:
            for info in self.__apis[instance].listFiles(dataset=dataset):
                fn = info['logical_file_name']
                result.files[fn].lumis = [(-2, -2)]
        else:
            blocks = self.__apis[instance].listBlocks(dataset=dataset)
            if mask:
                unmasked_lumis = LumiList(filename=mask)
            for block in blocks:
                runs = self.__apis[instance].listFileLumis(block_name=block['block_name'])
                for run in runs:
                    fn = run['logical_file_name']
                    for lumi in run['lumi_section_num']:
                        if not mask or ((run['run_num'], lumi) in unmasked_lumis):
                            result.files[fn].lumis.append((run['run_num'], lumi))

        result.total_lumis = sum([len(f.lumis) for f in result.files.values()])
        result.masked_lumis = result.unmasked_lumis - result.total_lumis

        return result
