#!/usr/bin/env python
import urllib
import urllib2
import json

class DASInterface:
    def __init__(self, host='https://cmsweb.cern.ch', path='/das/cache', debug=1):
        self.host = host
        self.path = path
        self.debug = debug

    def get_files(self, dataset):
        query = 'file dataset = %s' % dataset
        url = self.host + self.path
        headers = {"Accept": "application/json"}
        parameters  = {'input':query, 'idx':0, 'limit':0, 'instance':'cms_dbs_ph_analysis_01'} #can make these arguments later
        encoded_data = urllib.urlencode(parameters, doseq=True)
        url += '?%s' % encoded_data
        handler = urllib2.HTTPHandler(debuglevel=self.debug)
        opener = urllib2.build_opener(handler)
        request  = urllib2.Request(url=url, headers=headers)

        director = opener.open(request)
        json_dict = json.loads(director.read())
        director.close()

        data = json_dict['data']
        dataset_files = [row['file'][0]['name'] for row in data]

        return dataset_files


