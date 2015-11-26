from collections import defaultdict
import math
import os
import re
import requests
from retrying import retry
import shutil
import tempfile
from lobster import util, fs

from dbs.apis.dbsClient import DbsApi
from WMCore.DataStructs.LumiList import LumiList


class DatasetInfo(object):
    def __init__(self):
        self.event_counts = defaultdict(int)
        self.file_based = False
        self.files = []
        self.filesizes = defaultdict(int)
        self.tasksize = 1
        self.lumis = defaultdict(list)
        self.total_events = 0
        self.total_lumis = 0
        self.unmasked_lumis = 0
        self.masked_lumis = 0

    def __repr__(self):
        descriptions = ['{a}={v}'.format(a=attribute, v=getattr(self, attribute)) for attribute in self.__dict__]
        return 'DatasetInfo({0})'.format(',\n'.join(descriptions))


class MetaInterface:
    def __init__(self):
        self.__file_interface = FileInterface()
        self.__das_interface = DASInterface()

    def get_info(self, cfg):
        info = None
        if 'dataset' in cfg:
            info = self.__das_interface.get_info(cfg)
        else:
            info = self.__file_interface.get_info(cfg)
        info.path = cfg['label']
        return info


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


class DASInterface:
    def __init__(self):
        self.__apis = {}
        self.__dsets = {}
        self.__cache = tempfile.mkdtemp()

    def __del__(self):
        shutil.rmtree(self.__cache)

    def __get_mask(self, url, cfg):
        if not re.match(r'https?://', url):
            return util.findpath(cfg['basedirs'], url)

        fn = os.path.basename(url)
        cached = os.path.join(self.__cache, fn)
        if not os.path.isfile(cached):
            r = requests.get(url)
            if not r.ok:
                raise IOError("unable to retrieve '{0}'".format(url))
            with open(cached, 'w') as f:
                f.write(r.text)
        return cached

    def get_info(self, cfg):
        dataset = cfg['dataset']
        if dataset not in self.__dsets:
            instance = cfg.get('dbs instance', 'global')
            mask = cfg.get('lumi mask')
            if mask:
                mask = self.__get_mask(mask, cfg)
            file_based = cfg.get('file based', False)
            res = self.query_database(dataset, instance, mask, file_based)

            num = cfg.get('events per task')
            if num:
                res.tasksize = int(math.ceil(num / float(res.total_events) * res.total_lumis))
            else:
                res.tasksize = cfg.get('lumis per task', 25)

            self.__dsets[dataset] = res

        return self.__dsets[dataset]

    def query_database(self, dataset, instance, mask, file_based):
        if instance not in self.__apis:
            dbs_url = 'https://cmsweb.cern.ch/dbs/prod/{0}/DBSReader'.format(instance)
            self.__apis[instance] = DASWrapper(dbs_url)

        result = DatasetInfo()

        infos = self.__apis[instance].listFileSummaries(dataset=dataset)
        result.total_events = sum([info['num_event'] for info in infos])
        result.unmasked_lumis = sum([info['num_lumi'] for info in infos])

        for info in self.__apis[instance].listFiles(dataset=dataset, detail=True):
            result.event_counts[info['logical_file_name']] = info['event_count']
            result.filesizes[info['logical_file_name']] = info['file_size']

        files = set()
        if file_based:
            for file in self.__apis[instance].listFiles(dataset=dataset):
                filename = file['logical_file_name']
                files.add(filename)
                result.lumis[filename] = [(-2, -2)]
        else:
            blocks = self.__apis[instance].listBlocks(dataset=dataset)
            if mask:
                unmasked_lumis = LumiList(filename=mask)
            for block in blocks:
                runs = self.__apis[instance].listFileLumis(block_name=block['block_name'])
                for run in runs:
                    file = run['logical_file_name']
                    for lumi in run['lumi_section_num']:
                        if not mask or ((run['run_num'], lumi) in unmasked_lumis):
                            result.lumis[file].append((run['run_num'], lumi))
                    if file in result.lumis:
                        files.add(file)

        result.files = list(files)
        result.total_lumis = len(sum([result.lumis[f] for f in result.files], []))
        result.masked_lumis = result.unmasked_lumis - result.total_lumis

        return result


class FileInterface:
    def __init__(self):
        self.__dsets = {}

    def get_info(self, cfg):
        label = cfg['label']
        files = cfg.get('files', None)

        if label not in self.__dsets:
            dset = DatasetInfo()
            dset.file_based = True

            if not files:
                ntasks = cfg.get('num tasks', 1)
                nlumis = 1
                if 'events per lumi' in cfg:
                    nlumis = int(math.ceil(float(cfg['events per task']) / cfg['events per lumi']))
                dset.files = [None]
                dset.lumis[None] = [(1, x) for x in range(1, ntasks * nlumis + 1, nlumis)]
                dset.total_lumis = cfg.get('num tasks', 1)

                # we don't cache gen-tasks (avoid overwriting num tasks
                # etc...)
            else:
                dset.tasksize = cfg.get("files per task", 1)
                if not isinstance(files, list):
                    files = [files]
                for entry in files:
                    entry = os.path.expanduser(entry)
                    if fs.isdir(entry):
                        dset.files += filter(fs.isfile, fs.ls(entry))

                dset.total_lumis = len(dset.files)

                for file in dset.files:
                    # hack because it will be slow to open all the input files to read the run/lumi info
                    dset.lumis[file] = [(-1, -1)]
                    dset.filesizes[file] = fs.getsize(file)
            self.__dsets[label] = dset

        return self.__dsets[label]
