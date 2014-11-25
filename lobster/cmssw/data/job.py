#!/usr/bin/env python

from contextlib import contextmanager
from datetime import datetime
import gzip
import json
import os
import shutil
import subprocess
import sys
import traceback

sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_5/external')

from DashboardAPI import apmonSend, apmonFree
from FWCore.PythonUtilities.LumiList import LumiList
from ProdCommon.FwkJobRep.ReportParser import readJobReport

fragment = """
import FWCore.ParameterSet.Config as cms
process.Timing = cms.Service("Timing",
    useJobReport = cms.untracked.bool(True),
    summaryOnly = cms.untracked.bool(True))
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32({events}))
"""

sum_frag = """
if hasattr(process, 'options'):
    process.options.wantSummary = cms.untracked.bool(True)
else:
    process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))
"""

def calculate_alder32(data, config):
    """Try to calculate checksums for output files.
    """
    res = {}
    for local, remote in config['output files']:
        try:
            p = subprocess.Popen(['edmFileUtil', '-a', local], stdout=subprocess.PIPE)
            stdout = p.communicate()[0]

            if p.returncode == 0:
                res[os.path.basename(remote)] = stdout.split()[-2]
        except:
            pass
    return res

@contextmanager
def check_execution(data, code):
    """Check execution within context.

    Updates 'job exit code' in `data`, if not already set, and prints a
    stack trace if the yield fails with an exception.
    """
    try:
        yield
    except:
        print traceback.format_exc()
        if data['job exit code'] == 0:
            data['job exit code'] = code

def check_outputs(config):
    """Check that chirp received the output files.
    """
    chirp_server = config.get('chirp server', None)

    if not chirp_server:
        return True

    for local, remote in config['output files']:
        size = os.path.getsize(local)
        p = subprocess.Popen([
            os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp"),
            chirp_server, "stat", remote], stdout=subprocess.PIPE)
        stdout = p.communicate()[0]
        for l in stdout.splitlines():
            if l.startswith('size:'):
                if int(l.split()[1]) != size:
                    print "> size mismatch after transfer for " + local
                    return False
                break
        else:
            # size: is not in stdout
            return False
    return True

def copy_inputs(config):
    """Copies input files if desired.

    Checks the passed configuration for transfer settings and modifies the
    input file mask to point to transferred files, where appropriate.
    Local access is checked first, followed by (not yet implemented) xrootd
    access, and finally, attempting to transfer files via chirp.
    """
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
            else:
                raise IOError("Could not transfer file {0}".format(cfile))
            continue

        # FIXME remove with xrootd test?
        # add file if not local or in chirp and then hope that CMSSW can
        # access it
        config['mask']['files'].append(file)

    print "--- modified input files:"
    for fn in config['mask']['files']:
        print fn
    print "---"

def copy_outputs(data, config):
    """Copy output files.

    If the job failed, delete output files, to avoid work_queue
    transferring them.  Otherwise, if a chirp server is specified, transfer
    output files out via chirp.  In any case, file sizes are added up and
    inserted into the job data.
    """
    server = config.get('chirp server', None)
    outsize = 0
    for localname, remotename in config['output files']:
        # prevent stageout of data for failed jobs
        if os.path.exists(localname) and data['cmssw exit code'] != 0:
            os.remove(localname)
            continue
        elif data['cmssw exit code'] != 0:
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
            if status != 0:
                data['stageout exit code'] = status
                raise IOError("Failed to transfer output file '{0}'".format(localname))
    data['output size'] = outsize

def edit_process_source(pset, config, events=-1):
    """Edit parameter set for job.

    Adjust input files and lumi mask, as well as adding a process summary
    for performance analysis.
    """
    files = config['mask']['files']
    lumis = LumiList(compactList=config['mask']['lumis']).getVLuminosityBlockRange()
    want_summary = config['want summary']

    with open(pset, 'a') as fp:
        frag = fragment.format(events=events)
        if any([f for f in files]):
            frag += "\nprocess.source.fileNames = cms.untracked.vstring({0})".format(repr([str(f) for f in files]))
        if lumis:
            frag += "\nprocess.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({0})".format([str(l) for l in lumis])
        if want_summary:
            frag += sum_frag

        print "--- config file fragment:"
        print frag
        print "---"
        fp.write(frag)

def extract_info(data, report_filename):
    """Extract job data from a framework report.

    Analyze the CMSSW job framework report to get the CMSSW exit code,
    skipped files, runs and lumis processed on a file basis, total events
    written, and CPU time overall and per event.
    """
    exit_code = 0
    skipped = []
    infos = {}
    written = 0

    with open(report_filename) as f:
        for report in readJobReport(f):
            for error in report.errors:
                code = error.get('ExitStatus', exit_code)
                if exit_code == 0:
                    exit_code = code

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

    data['files']['info'] = infos
    data['files']['skipped'] = skipped
    data['events written'] = written
    data['cmssw exit code'] = exit_code
    # For efficiency, we care only about the CPU time spent processing
    # events
    data['cpu time'] = eventtime

    return cputime

def extract_time(filename):
    """Load file contents as integer timestamp.
    """
    with open(filename) as f:
        return int(f.readline())

def extract_cmssw_times(log_filename, default=None):
    """Get time information from a CMSSW stdout.

    Extracts the first time a file opening is initialized and performed,
    and the time the first event is processed.
    """
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

data = {
    'files': {
        'adler32': {},
        'info': {},
        'skipped': [],
    },
    'job exit code': 0,
    'cmssw exit code': 0,
    'stageout exit code': 0,
    'cpu time': 0,
    'events written': 0,
    'output size': 0,
    'task timing info': [None] * 5
}

(pset, configfile) = sys.argv[1:]
with open(configfile) as f:
    config = json.load(f)

with check_execution(data, 179):
    copy_inputs(config)

monitorid = config['monitoring']['monitorid']
syncid = config['monitoring']['syncid']
taskid = config['monitoring']['taskid']

args = config['arguments']

pset_mod = pset.replace(".py", "_mod.py")
shutil.copy2(pset, pset_mod)

env = os.environ
env['X509_USER_PROXY'] = 'proxy'

edit_process_source(pset_mod, config)

prologue = config.get('prologue', [])
epilogue = config.get('epilogue', [])

if len(prologue) > 0:
    print "--- prologue:"
    with check_execution(data, 180):
        subprocess.check_call(prologue, env=env)
    print "---"

#
# Start proper CMSSW job
#

apmonSend(taskid, monitorid, {
            'ExeStart': 'cmsRun',
            'SyncCE': 'ndcms.crc.nd.edu',
            'SyncGridJobId': syncid,
            'WNHostName': os.environ.get('HOSTNAME', '')
            })
apmonFree()

print "--- Running cmsRun"
print 'cmsRun -j report.xml "{0}" {1} > cmssw.log 2>&1'.format(pset_mod, ' '.join([repr(str(arg)) for arg in args]))
print "---"
data['job exit code'] = subprocess.call(
        'cmsRun -j report.xml "{0}" {1} > cmssw.log 2>&1'.format(pset_mod, ' '.join([repr(str(arg)) for arg in args])),
        shell=True, env=env)

apmonSend(taskid, monitorid, {'ExeEnd': 'cmsRun'})

cputime = 0
with check_execution(data, 190):
    cputime = extract_info(data, 'report.xml')

with check_execution(data, 191):
    data['task timing info'][:2] = [extract_time('t_wrapper_start'), extract_time('t_wrapper_ready')]

data['files']['adler32'] = calculate_alder32(data, config)

now = int(datetime.now().strftime('%s'))

with check_execution(data, 192):
    data['task timing info'][2:] = extract_cmssw_times('cmssw.log', now)

data['task timing info'].append(now)

#
# End proper CMSSW job
#

if len(epilogue) > 0:
    print "--- epilogue:"
    with check_execution(data, 199):
        subprocess.check_call(epilogue, env=env)
    print "---"

with check_execution(data, 210):
    copy_outputs(data, config)

if data['job exit code'] == 0 and not check_outputs(config):
    data['job exit code'] = 211
    data['outsize'] = 0

data['task timing info'].append(int(datetime.now().strftime('%s')))

with check_execution(data, 193):
    with open('report.json', 'w') as f:
        json.dump(data, f, indent=2)

for filename in 'cmssw.log report.xml'.split():
    if os.path.isfile(filename):
        with check_execution(data, 194):
            with open(filename) as f:
                zipf = gzip.open(filename + ".gz", "wb")
                zipf.writelines(f)
                zipf.close()

total_time = data['task timing info'][-1] - data['task timing info'][0]

exit_code = data['job exit code']
cmssw_exit_code = data['cmssw exit code']
stageout_exit_code = data['stageout exit code']

print "Execution time", str(total_time)

print "Exiting with code", str(exit_code)
print "Reporting ExeExitCode", str(cmssw_exit_code)
print "Reporting StageOutExitCode", str(stageout_exit_code)

apmonSend(taskid, monitorid, {
            'ExeTime': str(total_time),
            'ExeExitCode': str(cmssw_exit_code),
            'JobExitCode': str(exit_code),
            'JobExitReason': '',
            'StageOutSE': ' ndcms.crc.nd.edu',
            'StageOutExitStatus': str(stageout_exit_code),
            'StageOutExitStatusReason': 'Copy succedeed with srm-lcg utils',
            'CrabUserCpuTime': str(cputime),
            # 'CrabSysCpuTime': '5.91',
            # 'CrabCpuPercentage': '18%',
            'CrabWrapperTime': str(total_time),
            # 'CrabStageoutTime': '50',
            })
apmonFree()

sys.exit(exit_code)
