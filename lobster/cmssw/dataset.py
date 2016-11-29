import hashlib
import logging
import math
import os
import pickle
import re
import requests
from retrying import retry
import xdg.BaseDirectory

from lobster.core.dataset import DatasetInfo
from lobster.util import Configurable

from dbs.apis.dbsClient import DbsApi
from WMCore.Credential.Proxy import Proxy
from WMCore.DataStructs.LumiList import LumiList

logger = logging.getLogger('lobster.cmssw.dataset')


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
        self.cachedir = xdg.BaseDirectory.save_cache_path('lobster')

    def __cachename(self, name, mask):
        m = hashlib.sha256()
        m.update(name)
        if mask:
            m.update(mask)
        return os.path.join(self.cachedir,
                            "{}-{}.pkl".format(name.strip('/').split('/')[0], m.hexdigest()))

    def cache(self, name, mask, baseinfo, dataset):
        logger.debug("writing dataset '{}' to cache".format(name))
        with open(self.__cachename(name, mask), 'wb') as fd:
            pickle.dump((baseinfo, dataset), fd)

    def cached(self, name, mask, baseinfo):
        try:
            with open(self.__cachename(name, mask), 'rb') as fd:
                info, dset = pickle.load(fd)
                if baseinfo == info:
                    logger.debug("retrieved dataset '{}' from cache".format(name))
                    return dset
                return None
        except Exception:
            return None


class Dataset(Configurable):

    """
    Specification for processing a dataset stored in DBS.

    Parameters
    ----------
        dataset : str
            The full dataset name as in DBS.
        lumis_per_task : int
            How many luminosity sections to process in one task.  May be
            modified by Lobster to match the user-specified task runtime.
        events_per_task : int
            Adjust `lumis_per_task` to contain as many luminosity sections
            to process the specified amount of events.
        lumi_mask : str
            The URL or filename of a JSON luminosity section mask, as
            customary in CMS.
        file_based : bool
            Process whole files instead of single luminosity sections.
        dbs_instance : str
            Which DBS instance to query for the `dataset`.
    """
    _mutable = {}

    __apis = {}
    __dsets = {}
    __cache = Cache()

    def __init__(self, dataset, lumis_per_task=25, events_per_task=None, lumi_mask=None, file_based=False, dbs_instance='global'):
        self.dataset = dataset
        self.lumi_mask = lumi_mask
        self.lumis_per_task = lumis_per_task
        self.events_per_task = events_per_task
        self.file_based = file_based
        self.dbs_instance = 'https://cmsweb.cern.ch/dbs/prod/{0}/DBSReader'.format(dbs_instance)

        self.total_units = 0

    def __get_mask(self, url):
        if not re.match(r'https?://', url):
            return url

        fn = os.path.basename(url)
        cached = os.path.join(Dataset.__cache.cachedir, fn)
        if not os.path.isfile(cached):
            r = requests.get(url)
            if not r.ok:
                raise IOError("unable to retrieve '{0}'".format(url))
            with open(cached, 'w') as f:
                f.write(r.text)
        return cached

    def validate(self):
        if self.dataset in Dataset.__dsets:
            return True

        if self.lumi_mask:
            self.lumi_mask = self.__get_mask(self.lumi_mask)

        cred = Proxy({'logger': logging.getLogger("WMCore")})
        dbs = DASWrapper(self.dbs_instance, ca_info=cred.getProxyFilename())

        baseinfo = dbs.listFileSummaries(dataset=self.dataset)
        if baseinfo is None or (len(baseinfo) == 1 and baseinfo[0] is None):
            return False
        return True

    def get_info(self):
        if self.dataset not in Dataset.__dsets:
            if self.lumi_mask:
                self.lumi_mask = self.__get_mask(self.lumi_mask)
            res = self.query_database()

            if self.events_per_task:
                if res.total_events > 0:
                    res.tasksize = int(math.ceil(self.events_per_task / float(res.total_events) * res.total_units))
                else:
                    res.tasksize = 1
            else:
                res.tasksize = self.lumis_per_task

            Dataset.__dsets[self.dataset] = res

        self.total_units = Dataset.__dsets[self.dataset].total_units
        return Dataset.__dsets[self.dataset]

    def query_database(self):
        cred = Proxy({'logger': logging.getLogger("WMCore")})
        dbs = DASWrapper(self.dbs_instance, ca_info=cred.getProxyFilename())

        baseinfo = dbs.listFileSummaries(dataset=self.dataset)
        if baseinfo is None or (len(baseinfo) == 1 and baseinfo[0] is None):
            raise ValueError('unable to retrive information for dataset {}'.format(self.dataset))

        if not self.file_based:
            result = self.__cache.cached(self.dataset, self.lumi_mask, baseinfo)
            if result:
                return result

        result = DatasetInfo()
        result.total_events = sum([info['num_event'] for info in baseinfo])

        for info in dbs.listFiles(dataset=self.dataset, detail=True):
            fn = info['logical_file_name']
            result.files[fn].events = info['event_count']
            result.files[fn].size = info['file_size']

        if self.file_based:
            for info in dbs.listFiles(dataset=self.dataset):
                fn = info['logical_file_name']
                result.files[fn].lumis = [(-2, -2)]
        else:
            blocks = dbs.listBlocks(dataset=self.dataset)
            if self.lumi_mask:
                unmasked_lumis = LumiList(filename=self.lumi_mask)
            for block in blocks:
                runs = dbs.listFileLumis(block_name=block['block_name'])
                for run in runs:
                    fn = run['logical_file_name']
                    for lumi in run['lumi_section_num']:
                        if not self.lumi_mask or ((run['run_num'], lumi) in unmasked_lumis):
                            result.files[fn].lumis.append((run['run_num'], lumi))
                        elif self.lumi_mask and ((run['run_num'], lumi) not in unmasked_lumis):
                            result.masked_units += 1

        result.unmasked_units = sum([len(f.lumis) for f in result.files.values()])
        result.total_units = result.unmasked_units + result.masked_units

        if not self.file_based:
            self.__cache.cache(self.dataset, self.lumi_mask, baseinfo, result)

        return result
