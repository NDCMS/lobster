#!/usr/bin/env python
from DBSAPI.dbsApi import DbsApi

class DASInterface:
    def __init__(self, global_dbs_url='http://cmsdbsprod.cern.ch/cms_dbs_prod_global/servlet/DBSServlet'):
        self.api_reader = DbsApi({'url':global_dbs_url})
        self.files = {}
        self.events_per_file = {}

    def get_files(self, dataset):
        files = self.api_reader.listDatasetFiles(dataset)
        self.files[dataset] = [file['LogicalFileName'] for file in files]
        for file in files:
            self.events_per_file[file['LogicalFileName']] = file['NumberOfEvents']

        return self.files[dataset]

