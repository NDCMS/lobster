import os
import glob
import sys
from collections import defaultdict
sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_2_patch2/external/dbs3client')
from dbs.apis.dbsClient import DbsApi

class DatasetInfo():
    def __init__(self, label):
        self.label = label
        self.total_events = 0
        self.events = {}
        self.total_lumis = 0
        self.lumis = defaultdict(list)
        self.files = []

class DASInterface:
#    def __init__(self, config, global_dbs_url='https://cmsweb-testbed.cern.ch/dbs/int/global/DBSReader'):
    def __init__(self, config, global_dbs_url='https://cmsweb.cern.ch/dbs/prod/global/DBSReader'):
        self.ds_info = {}
        self.api_reader = DbsApi(global_dbs_url)
        self.datasets = {}
        for task in config['tasks']:
            self.datasets[task['dataset label']] = task['dataset']

    def __getitem__(self, label):
        if label not in self.ds_info.keys():
            self.ds_info[label] = DatasetInfo(label)
            self.query_database(label)

        return self.ds_info[label]

    def query_database(self, label):
        #TO DO: switch to applying json mask here
        dbs_output = self.api_reader.listFiles(dataset=self.datasets[label], detail=True)
        self.ds_info[label].files = [entry['logical_file_name'] for entry in dbs_output]
        self.ds_info[label].total_events = sum([entry['event_count'] for entry in dbs_output])
        for file in self.ds_info[label].files:
            for run in self.api_reader.listFileLumis(logical_file_name=file):
                self.ds_info[label].lumis[file] += [(run['run_num'], l) for l in run['lumi_section_num']]
            self.ds_info[label].total_lumis += len(self.ds_info[label].lumis[file])

class FileInterface:
    def __init__(self, config):
        self.ds_info = {}
        for task in config['tasks']:
            label = task['dataset label']
            files = task['files']
            ds_info =  DatasetInfo(label)
            if os.path.isdir(files):
                ds_info.files = ['file:'+f for f in glob.glob(os.path.join(files, '*'))]
            elif os.path.isfile(files):
                ds_info.files = ['file:'+f.strip() for f in open(files).readlines()]
            for file in ds_info.files:
                ds_info.lumis[file] = [(-1, -1)] # hack because it will be slow to open all the input files to read the run/lumi info

            self.ds_info[label] = ds_info

    def __getitem__(self, label):
        return self.ds_info[label]
