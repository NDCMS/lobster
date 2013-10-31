from DBSAPI.dbsApi import DbsApi
from FWCore.PythonUtilities.LumiList import LumiList

class DASInterface:
    def __init__(self, global_dbs_url='http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'):
        self.api_reader = DbsApi({'url':global_dbs_url})

    def __getitem__(self, dataset):
        if dataset not in self.__dict__.keys():
            self.__dict__[dataset] = DatasetInfo(dataset, self.api_reader)

        return self.__dict__[dataset]

class DatasetInfo():
    def __init__(self, dataset, reader):
        self.api_reader = reader
        self.dataset = dataset
        self.total_events = 0
        self.events = {}
        self.total_lumis = 0
        self.lumis = {}
        self.query_database()

    def query_database(self):
        dbs_output = self.api_reader.listFiles(self.dataset, retriveList=['retrive_lumi'])
#        dbs_output = self.api_reader.listDatasetFiles(self.dataset)
        self.files = [entry['LogicalFileName'] for entry in dbs_output]
        for entry in dbs_output:
            self.lumis[entry['LogicalFileName']] = [(x['RunNumber'], x['LumiSectionNumber']) for x in entry['LumiList']]
            self.events[entry['LogicalFileName']] = entry['NumberOfEvents']
            self.total_events += entry['NumberOfEvents']
            self.total_lumis += len(self.lumis[entry['LogicalFileName']])

