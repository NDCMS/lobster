#!/usr/bin/env python

from datetime import datetime
import gzip
import json
import os
import pickle
import shutil
import subprocess
import sys

sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_5/external')

from DashboardAPI import apmonSend, apmonFree
from FWCore.PythonUtilities.LumiList import LumiList
from ProdCommon.FwkJobRep.ReportParser import readJobReport

fragment = """import FWCore.ParameterSet.Config as cms
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32({events}))"""

def edit_process_source(cmssw_config_file, files, lumis, events=-1):
    with open(cmssw_config_file, 'a') as config:
        frag = fragment.format(events=events)
        if any([f for f in files]):
            frag += "\nprocess.source.fileNames = cms.untracked.vstring({input_files})".format(input_files=repr([str(f) for f in files]))
        if lumis:
            frag += "\nprocess.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({lumis})".format(lumis=[str(l) for l in lumis.getVLuminosityBlockRange()])
        print "--- config file fragment:"
        print frag
        print "---"
        config.write(frag)

def extract_info(report_filename):
    skipped = []
    lumis = []
    read = 0
    written = 0

    with open(report_filename) as f:
        for report in readJobReport(f):
            for file in report.skippedFiles:
                skipped.append(file['Lfn'])

            use_fileinfo = len(report.files) > 0
            for file in report.files:
                written += int(file['TotalEvents'])
                for run, ls in file['Runs'].items():
                    for lumi in ls:
                        lumis.append((run, lumi))

            for file in report.inputFiles:
                read += int(file['EventsRead'])
                if not use_fileinfo:
                    for run, ls in file['Runs'].items():
                        for lumi in ls:
                            lumis.append((run, lumi))

    return LumiList(lumis=lumis), skipped, read, written

def extract_time(filename):
    with open(filename) as f:
        return int(f.readline())

def extract_cmssw_times(log_filename, default=None):
    finit = default
    fopen = default
    first = default

    with open(log_filename) as f:
        for line in f.readlines():
            if finit == default and line[26:36] == "Initiating":
                finit = int(datetime.strptime(line[0:20], "%d-%b-%Y %X").strftime('%s'))
            elif fopen == default and line[26:38] == "Successfully":
                fopen = int(datetime.strptime(line[0:20], "%d-%b-%Y %X").strftime('%s'))
            elif first == default and line[21:24] == "1st":
                first = int(datetime.strptime(line[-29:-9], "%d-%b-%Y %X").strftime('%s'))

    return (finit, fopen, first)

(config, data) = sys.argv[1:]
with open(data, 'rb') as f:
    (args, files, lumis, taskid, monitorid, syncid) = pickle.load(f)

apmonSend(taskid, monitorid, {
            'ExeStart': 'cmsRun',
            'SyncCE': 'ndcms.crc.nd.edu',
            'SyncGridJobId': syncid,
            'WNHostName': os.environ.get('HOSTNAME', '')
            })
apmonFree()

configfile = config.replace(".py", "_mod.py")
shutil.copy2(config, configfile)

env = os.environ
env['X509_USER_PROXY'] = 'proxy'

edit_process_source(configfile, files, lumis)

# exit_code = subprocess.call('python "{0}" {1}'.format(configfile, ' '.join(map(repr, args))), shell=True, env=env)
exit_code = subprocess.call('cmsRun -j report.xml "{0}" {1} > cmssw.log 2>&1'.format(configfile, ' '.join(map(repr, args))), shell=True, env=env)

try:
    run_info, skipped, read, written = extract_info('report.xml')
except Exception as e:
    print e
    if exit_code == 0:
        exit_code = 190

try:
    times = [extract_time('t_wrapper_start'), extract_time('t_wrapper_ready')]
except Exception as e:
    print e
    times = [None, None]
    if exit_code == 0:
        exit_code = 191

now = int(datetime.now().strftime('%s'))

try:
    times += extract_cmssw_times('cmssw.log', now)
except Exception as e:
    print e
    times += [None * 3]
    if exit_code == 0:
        exit_code = 192

times.append(now)

try:
    f = open('report.pkl', 'wb')
    pickle.dump((run_info, skipped, read, written, times), f, pickle.HIGHEST_PROTOCOL)
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

print "Execution time", str(times[-1] - times[0])
print "Exiting with code", str(exit_code)

apmonSend(taskid, monitorid, {
            'ExeEnd': 'cmsRun',
            'ExeTime': str(times[-1] - times[0]),
            'ExeExitCode': str(exit_code),
            'StageOutSE': ' ndcms.crc.nd.edu',
            'StageOutExitStatus': '0',
            'StageOutExitStatusReason': 'Copy succedeed with srm-lcg utils',
            # 'CrabUserCpuTime': '178.08',
            # 'CrabSysCpuTime': '5.91',
            # 'CrabCpuPercentage': '18%',
            # 'CrabWrapperTime': '1074',
            # 'CrabStageoutTime': '50',
            })
apmonFree()

sys.exit(exit_code)
