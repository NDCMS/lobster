#!/usr/bin/env python

from datetime import datetime
import gzip
import json
import os
import pickle
import shutil
import subprocess
import sys
import xml.dom.minidom

from FWCore.PythonUtilities.LumiList import LumiList

fragment = """import FWCore.ParameterSet.Config as cms
process.source.fileNames = cms.untracked.vstring({input_files})
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))"""

def edit_process_source(cmssw_config_file, files, lumis):
    with open(cmssw_config_file, 'a') as config:
        frag = fragment.format(input_files=repr([str(f) for f in files]))
        if lumis:
            frag += "\nprocess.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({lumis})".format(lumis=[str(l) for l in lumis.getVLuminosityBlockRange()])
        print "--- config file fragment:"
        print frag
        print "---"
        config.write(frag)

def extract_processed_lumis(report_filename):
    dom = xml.dom.minidom.parse(report_filename)
    runs = dom.getElementsByTagName("File")[0].getElementsByTagName("Run")

    lumis = []
    for run in runs:
        run_number = int(run.getAttribute("ID"))
        lumi_sections = []
        for lumi in run.getElementsByTagName("LumiSection"):
            lumis.append((run_number, int(lumi.getAttribute("ID"))))
    return LumiList(lumis=lumis)

def extract_time(filename):
    try:
        with open(filename) as f:
            return int(f.readline())
    except Exception as e:
        print e
        return None

def extract_cmssw_times(log_filename):
    finit = None
    fopen = None
    first = None

    with open(log_filename) as f:
        for line in f.readlines():
            if not finit and line[26:36] == "Initiating":
                finit = int(datetime.strptime(line[0:20], "%d-%b-%Y %X").strftime('%s'))
            elif not fopen and line[26:38] == "Successfully":
                fopen = int(datetime.strptime(line[0:20], "%d-%b-%Y %X").strftime('%s'))
            elif not first and line[21:24] == "1st":
                first = int(datetime.strptime(line[-29:-9], "%d-%b-%Y %X").strftime('%s'))

    return (finit, fopen, first)

(config, data) = sys.argv[1:]
with open(data, 'rb') as f:
    (args, files, lumis) = pickle.load(f)

configfile = config.replace(".py", "_mod.py")
shutil.copy2(config, configfile)

env = os.environ
env['X509_USER_PROXY'] = 'proxy'

edit_process_source(configfile, files, lumis)

# exit_code = subprocess.call('python "{0}" {1}'.format(configfile, ' '.join(map(repr, args))), shell=True, env=env)
exit_code = subprocess.call('cmsRun -j report.xml "{0}" {1} > cmssw.log 2>&1'.format(configfile, ' '.join(map(repr, args))), shell=True, env=env)

try:
    run_info = extract_processed_lumis('report.xml')
    with open('processed.pkl', 'wb') as f:
        pickle.dump(run_info, f, pickle.HIGHEST_PROTOCOL)
except Exception as e:
    print e
    if exit_code == 0:
        exit_code = 190

try:
    times = [extract_time('t_wrapper_start'), extract_time('t_wrapper_ready')]
except Exception as e:
    print e
    if exit_code == 0:
        exit_code = 191

try:
    times += extract_cmssw_times('cmssw.log')
except Exception as e:
    print e
    times += [None * 3]
    if exit_code == 0:
        exit_code = 192

times.append(int(datetime.now().strftime('%s')))

try:
    f = open('times.pkl', 'wb')
    pickle.dump(times, f, pickle.HIGHEST_PROTOCOL)
except Exception as e:
    print e
    if exit_code == 0:
        exit_code = 193
finally:
    f.close()

for filename in 'cmssw.log report.xml'.split():
    if os.path.isfile(filename):
        try:
            with open(filename) as f:
                zipf = gzip.open(filename + ".gz", "wb")
                zipf.writelines(f)
                zipf.close()
        except Exception as e:
            print e
            if exit_code == 0:
                exit_code = 194

sys.exit(exit_code)
