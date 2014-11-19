#!/usr/bin/env python

from datetime import datetime
import gzip
import json
import os
import shutil
import subprocess
import sys

sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_5/external')

from DashboardAPI import apmonSend, apmonFree
from ProdCommon.FwkJobRep.ReportParser import readJobReport

fragment = """import FWCore.ParameterSet.Config as cms
process.Timing = cms.Service("Timing",
    useJobReport = cms.untracked.bool(True),
    summaryOnly = cms.untracked.bool(True))
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32({events}))"""

sum_frag = """\nif hasattr(process, 'options'):
    process.options.wantSummary = cms.untracked.bool(True)
else:
    process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))"""

def copy_inputs(config):
    if not config.get('transfer inputs', False):
        return

    chirp_server = config.get('chirp server', None)
    chirp_prefix = config.get('chirp prefix', None)
    lfn_prefix = config.get('lfn prefix')

    files = list(config['mask']['files'])
    config['mask']['files'] = []

    for file in [f.replace("file:", "") for f in files]:
        # pfile = lfn_prefix + file
        pfile = file
        if os.path.exists(pfile) and os.access(pfile, os.R_OK) and not os.path.isdir(pfile):
            config['mask']['files'].append('file:' + pfile)
            continue

        # TODO xrootd test

        if chirp_server and chirp_prefix:
            if file.startswith(chirp_prefix):
                cfile = file.replace(chirp_prefix, '', 1)
            else:
                cfile = file

            lfile = os.path.basename(lfile)

            status = subprocess.call([
                os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_get"),
                "-a",
                "globus",
                options.chirp,
                cfile,
                lfile])

            if status == 0:
                config['mask']['files'].append('file:' + lfile)
            continue

        # FIXME remove with xrootd test?
        # add file if not local or in chirp and then hope that CMSSW can
        # access it
        config['mask']['files'].append(file)

    print "--- modified input files:"
    for fn in config['mask']['files']:
        print fn
    print "---"

def edit_process_source(cmssw_config_file, files, lumis, want_summary, events=-1):
    with open(cmssw_config_file, 'a') as config:
        frag = fragment.format(events=events)
        if any([f for f in files]):
            frag += "\nprocess.source.fileNames = cms.untracked.vstring({input_files})".format(input_files=repr([str(f) for f in files]))
        if lumis:
            frag += "\nprocess.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({lumis})".format(lumis=[str(l) for l in lumis])
        if want_summary:
            frag += sum_frag

        print "--- config file fragment:"
        print frag
        print "---"
        config.write(frag)

def extract_info(report_filename):
    exit_code = 0
    skipped = []
    infos = {}
    written = 0

    with open(report_filename) as f:
        for report in readJobReport(f):
            for error in report.errors:
                exit_code = error.get('ExitStatus', exit_code)

            for file in report.skippedFiles:
                skipped.append(file['Lfn'])

            for file in report.files:
                written += int(file['TotalEvents'])

            for file in report.inputFiles:
                filename = file['LFN'] if len(file['LFN']) > 0 else file['PFN']
                file_lumis = []
                try:
                    for run, ls in file['Runs'].items():
                        for lumi in ls:
                            file_lumis.append((run, lumi))
                except AttributeError:
                    print 'Detected file-based job.'
                infos[filename] = (int(file['EventsRead']), file_lumis)
            eventtime = report.performance.summaries['Timing']['TotalEventCPU']
            cputime = report.performance.summaries['Timing']['TotalJobCPU']

    return infos, skipped, written, exit_code, eventtime, cputime

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

(pset, configfile) = sys.argv[1:]
with open(configfile) as f:
    config = json.load(f)

copy_inputs(config)

files = config['mask']['files']
lumis = config['mask']['lumis']

monitorid = config['monitoring']['monitorid']
syncid = config['monitoring']['syncid']
taskid = config['monitoring']['taskid']

args = config['arguments']
server = config.get('chirp server', None)
stageout = config['output files']
want_summary = config['want summary']

apmonSend(taskid, monitorid, {
            'ExeStart': 'cmsRun',
            'SyncCE': 'ndcms.crc.nd.edu',
            'SyncGridJobId': syncid,
            'WNHostName': os.environ.get('HOSTNAME', '')
            })
apmonFree()

pset_mod = pset.replace(".py", "_mod.py")
shutil.copy2(pset, pset_mod)

env = os.environ
env['X509_USER_PROXY'] = 'proxy'

edit_process_source(pset_mod, files, lumis, want_summary)

# exit_code = subprocess.call('python "{0}" {1}'.format(pset_mod, ' '.join(map(repr, args))), shell=True, env=env)
print "--- Running cmsRun"
print 'cmsRun -j report.xml "{0}" {1} > cmssw.log 2>&1'.format(pset_mod, ' '.join([repr(str(arg)) for arg in args]))
print "---"
exit_code = subprocess.call(
        'cmsRun -j report.xml "{0}" {1} > cmssw.log 2>&1'.format(pset_mod, ' '.join([repr(str(arg)) for arg in args])),
        shell=True, env=env)

apmonSend(taskid, monitorid, {'ExeEnd': 'cmsRun'})

try:
    files_info, files_skipped, events_written, cmssw_exit_code, eventtime, cputime = extract_info('report.xml')
except Exception as e:
    print e

    if exit_code == 0:
        exit_code = 190

    files_info = {}
    files_skipped = []
    events_written = 0
    cmssw_exit_code = 190
    eventtime = 0
    cputime = 0

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

stageout_exit_code = 0
outsize = 0

for localname, remotename in stageout:
    if os.path.exists(localname):
        if not cmssw_exit_code == 0:
            os.remove(localname)
            continue

        outsize += os.path.getsize(localname)

        if server:
            status = subprocess.call([os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_put"),
                                      "-a",
                                      "globus",
                                      "-d",
                                      "all",
                                      localname,
                                      server,
                                      remotename])
            if status != 0 and stageout_exit_code == 0:
                stageout_exit_code = status
if stageout_exit_code != 0:
    exit_code = 210

times.append(int(datetime.now().strftime('%s')))

try:
    f = open('report.json', 'w')
    json.dump({
        'files': {
            'info': files_info,
            'skipped': files_skipped,
        },
        'cmssw exit code': cmssw_exit_code,
        'task timing info': times,
        'cpu time': eventtime,
        'events written': events_written,
        'output size': outsize
    }, f, indent=2)
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
print "Reporting ExeExitCode", str(cmssw_exit_code)
print "Reporting StageOutExitCode", str(stageout_exit_code)

apmonSend(taskid, monitorid, {
            'ExeTime': str(times[-1] - times[0]),
            'ExeExitCode': str(cmssw_exit_code),
            'JobExitCode': str(exit_code),
            'JobExitReason': '',
            'StageOutSE': ' ndcms.crc.nd.edu',
            'StageOutExitStatus': str(stageout_exit_code),
            'StageOutExitStatusReason': 'Copy succedeed with srm-lcg utils',
            'CrabUserCpuTime': str(cputime),
            # 'CrabSysCpuTime': '5.91',
            # 'CrabCpuPercentage': '18%',
            'CrabWrapperTime': str(times[-1] - times[0]),
            # 'CrabStageoutTime': '50',
            })
apmonFree()

sys.exit(exit_code)
