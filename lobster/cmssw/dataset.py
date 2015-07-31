from collections import defaultdict
import math
import os
import sys
from lobster import util, fs

sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_2_patch2/external/dbs3client')
from dbs.apis.dbsClient import DbsApi
from FWCore.PythonUtilities.LumiList import LumiList

class DatasetInfo():
    def __init__(self):
        self.event_counts = defaultdict(int)
        self.file_based = False
        self.empty_source = False
        self.files = []
        self.filesizes = defaultdict(int)
        self.jobsize = 1
        self.lumis = defaultdict(list)
        self.total_events = 0
        self.total_lumis = 0
        self.unmasked_lumis = 0
        self.masked_lumis = 0

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

class DASInterface:
    def __init__(self):
        self.__apis = {}
        self.__dsets = {}

    def get_info(self, cfg):
        dataset = cfg['dataset']
        if dataset not in self.__dsets:
            instance = cfg.get('dbs instance', 'global')
            mask = util.findpath(cfg['basedirs'], cfg['lumi mask']) if cfg.get('lumi mask') else None
            file_based = cfg.get('file based', False)
            res = self.query_database(dataset, instance, mask, file_based)

            num = cfg.get('events per job')
            if num:
                res.jobsize = int(math.ceil(num / float(res.total_events) * res.total_lumis))
            else:
                res.jobsize = cfg.get('lumis per job', 25)

            self.__dsets[dataset] = res

        return self.__dsets[dataset]

    def query_database(self, dataset, instance, mask, file_based):
        if instance not in self.__apis:
            dbs_url = 'https://cmsweb.cern.ch/dbs/prod/{0}/DBSReader'.format(instance)
            self.__apis[instance] = DbsApi(dbs_url)

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
                    if result.lumis.has_key(file):
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
            dset.empty_source = cfg.get('empty source', False)

            if not files:
                dset.files = [None for x in range(cfg.get('num jobs', 1))]
                dset.lumis[None] = [(-1, -1)]
                dset.total_lumis = cfg.get('num jobs', 1)
                dset.empty_source = True

                # we don't cache gen-jobs (avoid overwriting num jobs
                # etc...)
            else:
                dset.jobsize = cfg.get("files per job", 1)
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
