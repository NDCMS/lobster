import os
import glob
from DBSAPI.dbsApi import DbsApi

class DatasetInfo():
    def __init__(self, label):
        self.label = label
        self.total_events = 0
        self.events = {}
        self.total_lumis = 0
        self.lumis = {}
        self.files = []

class DASInterface:
    def __init__(self, config, global_dbs_url='http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'):
        self.ds_info = {}
        self.api_reader = DbsApi({'url':global_dbs_url})
        self.datasets = {}
        for task in config['tasks']:
            self.datasets[task['dataset label']] = task['dataset']

    def __getitem__(self, label):
        if label not in self.ds_info.keys():
            self.ds_info[label] = DatasetInfo(label)
            self.query_database(label)

        return self.ds_info[label]

    def query_database(self, label):
        dbs_output = self.api_reader.listFiles(self.datasets[label], retriveList=['retrive_lumi'])
#        dbs_output = self.api_reader.listDatasetFiles(self.dataset)
        self.ds_info[label].files = [entry['LogicalFileName'] for entry in dbs_output]
        for entry in dbs_output:
            self.ds_info[label].lumis[entry['LogicalFileName']] = [(x['RunNumber'], x['LumiSectionNumber']) for x in entry['LumiList']]
            self.ds_info[label].events[entry['LogicalFileName']] = entry['NumberOfEvents']
            self.ds_info[label].total_events += entry['NumberOfEvents']
            self.ds_info[label].total_lumis += len(self.ds_info[label].lumis[entry['LogicalFileName']])

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
