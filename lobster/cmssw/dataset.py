from collections import defaultdict
import glob
import math
import os
import sys
from lobster import util

sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_2_patch2/external/dbs3client')
from dbs.apis.dbsClient import DbsApi
from FWCore.PythonUtilities.LumiList import LumiList

class DatasetInfo():
    def __init__(self):
        self.events = {}
        self.event_counts = defaultdict(int)
        self.file_based = False
        self.files = []
        self.filesizes = defaultdict(int)
        self.jobsize = 1
        self.lumis = defaultdict(list)
        self.total_events = 0
        self.total_lumis = 0

class MetaInterface:
    def __init__(self):
        self.__file_interface = FileInterface()
        self.__das_interface = DASInterface()

    def get_info(self, cfg):
        if 'dataset' in cfg:
            return self.__das_interface.get_info(cfg)
        else:
            return self.__file_interface.get_info(cfg)

class DASInterface:
    def __init__(self):
        self.__apis = {}
        self.__dsets = {}

    def get_info(self, cfg):
        dataset = cfg['dataset']
        if dataset not in self.__dsets:
            instance = cfg.get('dbs instance', 'global')
            mask = util.findpath(cfg['basedirs'], cfg['lumi mask']) if cfg.get('lumi mask') else None
            res = self.query_database(dataset, instance, mask)

            num = cfg.get('events per job')
            if num:
                res.jobsize = int(math.ceil(num / float(res.total_events) * res.total_lumis))
            else:
                res.jobsize = cfg.get('lumis per job', 25)

            self.__dsets[dataset] = res

        return self.__dsets[dataset]

    def query_database(self, dataset, instance, mask):
        if instance not in self.__apis:
            dbs_url = 'https://cmsweb.cern.ch/dbs/prod/{0}/DBSReader'.format(instance)
            self.__apis[instance] = DbsApi(dbs_url)

        result = DatasetInfo()

        infos = self.__apis[instance].listFileSummaries(dataset=dataset)
        result.total_events = sum([info['num_event'] for info in infos])

        for info in self.__apis[instance].listFiles(dataset=dataset, detail=True):
            result.event_counts[info['logical_file_name']] = info['event_count']
            result.filesizes[info['logical_file_name']] = info['file_size']

        files = set()
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
                if result.lumis.has_key(file):
                    files.add(file)

        result.files = list(files)
        result.total_lumis = len(sum([result.lumis[f] for f in result.files], []))

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
                dset.files = [None for x in range(cfg.get('num jobs', 1))]
                dset.lumis[None] = [(-1, -1)]
                dset.total_lumis = cfg.get('num jobs', 1)

                # we don't cache gen-jobs (avoid overwriting num jobs
                # etc...)
            else:
                dset.jobsize = cfg.get("files per job", 1)

                if os.path.isdir(files):
                    dset.files = ['file:'+f for f in glob.glob(os.path.join(files, '*'))]
                elif os.path.isfile(files):
                    dset.files = ['file:'+f.strip() for f in open(files).readlines()]
                elif isinstance(files, str):
                    dset.files = ['file:'+f for f in glob.glob(os.path.join(files))]

                dset.total_lumis = len(dset.files)
                for file in dset.files:
                    # hack because it will be slow to open all the input files to read the run/lumi info
                    dset.lumis[file] = [(-1, -1)]
                    dset.filesizes[file] = os.path.getsize(file[5:])

            self.__dsets[label] = dset

        return self.__dsets[label]
