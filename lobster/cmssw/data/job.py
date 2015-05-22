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
import ROOT
from ROOT import TFile

sys.path.insert(0, '/cvmfs/cms.cern.ch/crab/CRAB_2_10_5/external')

from DashboardAPI import apmonSend, apmonFree
from FWCore.PythonUtilities.LumiList import LumiList
from ProdCommon.FwkJobRep.ReportParser import readJobReport

ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gErrorIgnoreLevel = ROOT.kError
ROOT.gROOT.SetBatch(True)

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

def copy_inputs(data, config, env):
    """Copies input files if desired.

    Checks the passed configuration for transfer settings and modifies the
    input file mask to point to transferred files, where appropriate.
    Local access is checked first, followed by xrootd access, and finally,
    attempting to transfer files via chirp.
    """
    config['file map'] = {}

    chirp_server = config.get('chirp server', None)
    chirp_root = config.get('chirp root', None)

    xrootd_server = config.get('xrootd server', None)
    xrootd_root = config.get('xrootd root', None)

    files = list(config['mask']['files'])
    config['mask']['files'] = []

    for file in files:
        # File is locally accessible
        if os.path.exists(file) and os.access(file, os.R_OK) and not os.path.isdir(file):
            filename = 'file:' + file
            config['mask']['files'].append(filename)
            config['file map'][filename] = file
            continue

        # File has been transferred via WQ
        if os.path.exists(os.path.basename(file)):
            filename = 'file:' + os.path.basename(file)
            config['mask']['files'].append(filename)
            config['file map'][filename] = file
            continue

        if config.get('transfer inputs', False):
            if xrootd_server and xrootd_root:
                if file.startswith(xrootd_root):
                    xfile = file.replace(xrootd_root, '', 1)
                    filename = 'root://{server}/{file}'.format(server=xrootd_server, file=xfile)
                    config['mask']['files'].append(filename)
                    config['file map'][filename] = file
                    continue

            if chirp_server and chirp_root:
                if file.startswith(chirp_root):
                    cfile = file.replace(chirp_root, '', 1)
                else:
                    cfile = file

                lfile = os.path.basename(file)

                status = subprocess.call([
                    os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_get"),
                    "-a",
                    "globus",
                    "-d",
                    "all",
                    chirp_server,
                    cfile,
                    lfile], env=env)

                if status == 0:
                    filename = 'file:' + lfile
                    config['mask']['files'].append(filename)
                    config['file map'][filename] = file
                else:
                    raise IOError("Could not transfer file {0}".format(cfile))
                continue

        # FIXME remove with xrootd test?
        # add file if not local or in chirp and then hope that CMSSW can
        # access it
        config['mask']['files'].append(file)
        config['file map'][file] = file

    if not config['mask']['files']:
        data['stagein exit code'] = status

    print "--- modified input files:"
    for fn in config['mask']['files']:
        print fn
    print "---"

def copy_outputs(data, config, env):
    """Copy output files.

    If the job failed, delete output files, to avoid work_queue
    transferring them.  Otherwise, if a chirp server is specified, transfer
    output files out via chirp.  In any case, file sizes are added up and
    inserted into the job data.
    """
    srm_server = config.get('srm server', None)
    srm_root = config.get('srm root', None)

    chirp_server = config.get('chirp server', None)
    chirp_root = config.get('chirp root', None)

    outsize = 0
    outsize_bare = 0

    files = list(config['output files'])
    config['output files'] = []

    for localname, remotename in files:
        # prevent stageout of data for failed jobs
        if os.path.exists(localname) and data['cmssw exit code'] != 0:
            os.remove(localname)
            continue
        elif data['cmssw exit code'] != 0:
            continue

        outsize += os.path.getsize(localname)

        # using try just in case. Successful jobs should always
        # have an existing Events::TTree though.
        try:
            outsize_bare += get_bare_size(localname)
        except IOError as error:
            print error
            outsize_bare += os.path.getsize(localname)

        if os.path.isdir(os.path.dirname(remotename)):
            shutil.copy2(localname, remotename)
        elif srm_server:
            if srm_root and remotename.startswith(srm_root):
                remotename = remotename.replace(srm_root, '', 1)
            if remotename.startswith('/'):
                remotename = remotename[1:]

            prg = []

            if len(os.environ["LOBSTER_LCG_CP"]) > 0:
                prg = [os.environ["LOBSTER_LCG_CP"], "-b", "-v", "-D", "srmv2"]
            elif len(os.environ["LOBSTER_GFAL_COPY"]) > 0:
                # FIXME gfal is very picky about its environment
                prg = [os.environ["LOBSTER_GFAL_COPY"]]
            else:
                raise RuntimeError("no stage-out method available")

            args = prg + [
                "file:///" + os.path.join(os.getcwd(), localname),
                os.path.join(srm_server, remotename)
            ]

            print "--- staging-out with:"
            print " ".join(args)
            print "---"

            # FIXME is this really needed after simplifying the repos?
            pruned_env = dict(env)
            # for k in ['LD_LIBRARY_PATH', 'PATH']:
            #     pruned_env[k] = ':'.join([x for x in os.environ[k].split(':') if 'CMSSW' not in x])

            p = subprocess.Popen(args, env=pruned_env, stderr=subprocess.PIPE)
            p.wait()
            if p.returncode != 0:
                data['stageout exit code'] = p.returncode
                raise IOError("Failed to transfer output file '{0}':\n{1}".format(localname, p.stderr.read()))
            else:
                print p.stderr.read()
        elif chirp_server:
            if chirp_root and remotename.startswith(chirp_root):
                remotename = remotename.replace(chirp_root, '', 1)

            status = subprocess.call([os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_put"),
                                      "-a",
                                      "globus",
                                      "-d",
                                      "all",
                                      localname,
                                      chirp_server,
                                      remotename], env=env)
            if status != 0:
                data['stageout exit code'] = status
                raise IOError("Failed to transfer output file '{0}'".format(localname))

        config['output files'].append((localname, remotename))

    data['output size'] = outsize
    data['output bare size'] = outsize_bare

    print "--- modified output files:"
    for fn in config['output files']:
        print fn
    print "---"


def edit_process_source(pset, config):
    """Edit parameter set for job.

    Adjust input files and lumi mask, as well as adding a process summary
    for performance analysis.
    """
    files = config['mask']['files']
    lumis = LumiList(compactList=config['mask']['lumis']).getVLuminosityBlockRange()
    want_summary = config['want summary']

    with open(pset, 'a') as fp:
        frag = fragment.format(events=config['mask']['events'])
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

def extract_info(config, data, report_filename):
    """Extract job data from a framework report.

    Analyze the CMSSW job framework report to get the CMSSW exit code,
    skipped files, runs and lumis processed on a file basis, total events
    written, and CPU time overall and per event.
    """
    exit_code = 0
    skipped = []
    infos = {}
    written = 0
    eventsPerRun = 0

    with open(report_filename) as f:
        for report in readJobReport(f):
            for error in report.errors:
                code = error.get('ExitStatus', exit_code)
                if exit_code == 0:
                    exit_code = code

            for file in report.skippedFiles:
                filename = file['Lfn']
                filename = config['file map'].get(filename, filename)
                skipped.append(file['Lfn'])

            for file in report.files:
                written += int(file['TotalEvents'])

            for file in report.inputFiles:
                filename = file['LFN'] if len(file['LFN']) > 0 else file['PFN']
                filename = config['file map'].get(filename, filename)
                file_lumis = []
                try:
                    for run, ls in file['Runs'].items():
                        for lumi in ls:
                            file_lumis.append((run, lumi))
                except AttributeError:
                    print 'Detected file-based job.'
                infos[filename] = (int(file['EventsRead']), file_lumis)
                eventsPerRun += infos[filename][0]

            eventtime = report.performance.summaries['Timing']['TotalEventCPU']
            cputime = report.performance.summaries['Timing']['TotalJobCPU']

    data['files']['info'] = infos
    data['files']['skipped'] = skipped
    data['events written'] = written
    data['cmssw exit code'] = exit_code
    # For efficiency, we care only about the CPU time spent processing
    # events
    data['cpu time'] = eventtime
    data['events per run'] = eventsPerRun

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


def get_bare_size(filename):
    """Get the output bare size.

    Extracts Events->TTree::GetZipBytes()
    """
    rootfile = TFile(filename, "READ")
    if rootfile.IsZombie() or not rootfile.GetListOfKeys().Contains('Events'):
        raise IOError('The ROOT output file: {0} does not exist or does not contain TTree::Events'.format(filename))
    else:
        eventsTree = rootfile.Get("Events")
        events_size = eventsTree.GetZipBytes()
        rootfile.Close()
        return events_size

data = {
    'files': {
        'adler32': {},
        'info': {},
        'skipped': [],
    },
    'cache': {
        'start size': 0,
        'end size': 0,
        'type': None,
    },
    'job exit code': 0,
    'cmssw exit code': 0,
    'stageout exit code': 0,
    'cpu time': 0,
    'events written': 0,
    'output size': 0,
    'output bare size': 0,
    'task timing info': [None] * 7,
    'events per run': 0
}

env = os.environ
env['X509_USER_PROXY'] = 'proxy'

(pset, configfile) = sys.argv[1:]
with open(configfile) as f:
    config = json.load(f)

with check_execution(data, 179):
    copy_inputs(data, config, env)

data['task timing info'][2] = int(datetime.now().strftime('%s'))

# Dashboard does not like Unicode, just ASCII encoding
monitorid = str(config['monitoring']['monitorid'])
syncid = str(config['monitoring']['syncid'])
taskid = str(config['monitoring']['taskid'])

args = config['arguments']

pset_mod = pset.replace(".py", "_mod.py")
shutil.copy2(pset, pset_mod)

edit_process_source(pset_mod, config)

prologue = config.get('prologue', [])
epilogue = config.get('epilogue', [])

if len(prologue) > 0:
    print "--- prologue:"
    with check_execution(data, 180):
        subprocess.check_call(prologue, env=env)
    print "---"

data['task timing info'][3] = int(datetime.now().strftime('%s'))

#
# Start proper CMSSW job
#

parameters = {
            'ExeStart': 'cmsRun',
            'SyncCE': 'ndcms.crc.nd.edu',
            'SyncGridJobId': syncid,
            'WNHostName': os.environ.get('HOSTNAME', '')
            }

apmonSend(taskid, monitorid, parameters)
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
    cputime = extract_info(config, data, 'report.xml')

with check_execution(data, 191):
    data['task timing info'][:2] = [extract_time('t_wrapper_start'), extract_time('t_wrapper_ready')]

data['files']['adler32'] = calculate_alder32(data, config)

now = int(datetime.now().strftime('%s'))

with check_execution(data, 192):
    data['task timing info'][4:] = extract_cmssw_times('cmssw.log', now)

data['task timing info'].append(now)

#
# End proper CMSSW job
#

if len(epilogue) > 0:
    print "--- epilogue:"
    with check_execution(data, 199):
        subprocess.check_call(epilogue, env=env)
    print "---"

data['task timing info'].append(int(datetime.now().strftime('%s')))

with check_execution(data, 210):
    copy_outputs(data, config, env)

if data['job exit code'] == 0 and not check_outputs(config):
    data['job exit code'] = 211
    data['output size'] = 0

data['task timing info'].append(int(datetime.now().strftime('%s')))


if 'PARROT_ENABLED' in os.environ:
    data['cache']['type'] = int(os.path.isfile(os.path.join(os.environ['PARROT_CACHE'], 'hot_cache')))
else:
    data['cache']['type'] = 2

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
cmssw_wc_time = data['task timing info'][7] - data['task timing info'][3]
exit_code = data['job exit code']
cmssw_exit_code = data['cmssw exit code']
stageout_exit_code = data['stageout exit code']
events_per_run = data['events per run']

print "Execution time", str(total_time)

print "Exiting with code", str(exit_code)
print "Reporting ExeExitCode", str(cmssw_exit_code)
print "Reporting StageOutExitCode", str(stageout_exit_code)

parameters = {
            'ExeTime': str(cmssw_wc_time),
            'ExeExitCode': str(cmssw_exit_code),
            'JobExitCode': str(exit_code),
            'JobExitReason': '',
            'StageOutSE': 'ndcms.crc.nd.edu',
            'StageOutExitStatus': str(stageout_exit_code),
            'StageOutExitStatusReason': 'Copy succedeed with srm-lcg utils',
            'CrabUserCpuTime': str(cputime),
            # 'CrabSysCpuTime': '5.91',
            'CrabWrapperTime': str(total_time),
            # 'CrabStageoutTime': '50',
            'WCCPU': str(total_time),
            'NoEventsPerRun': str(events_per_run),
            'NbEvPerRun': str(events_per_run),
            'NEventsProcessed': str(events_per_run)
            }
try:
    parameters.update({'CrabCpuPercentage': str(float(cputime)/float(total_time))})
except:
    pass

apmonSend(taskid, monitorid, parameters)
apmonFree()

sys.exit(exit_code)
