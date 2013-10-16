#!/usr/bin/env python
import urllib, urllib2
import json
import time
import sys

class DASInterface:
    def __init__(self, host='https://cmsweb.cern.ch', path='/das/cache', debug=1):
        self.host = host
        self.path = path
        self.debug = debug

    def get_files(self, dataset, max_wait_time=120):
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
        data = director.read()
        director.close()

        start_time = time.time()
        while len(data) == 32:
            time.sleep(10)

            director = opener.open(request)
            data = director.read()
            director.close()

            elapsed_time = time.time() - start_time
            if elapsed_time > max_wait_time:
                print 'Problem connecting to DAS server.  Timed out after %s seconds.' % int(elapsed_time)
                sys.exit(99)

        json_dict = json.loads(data)
        dataset_files = [row['file'][0]['name'] for row in json_dict['data']]

        return dataset_files


