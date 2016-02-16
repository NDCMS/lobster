#!/usr/bin/env python

from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime
import gzip
import json
import logging
import os
import re
import resource
import shlex
import shutil
import subprocess
import sys
import traceback

sys.path.append('python')

from WMCore.DataStructs.LumiList import LumiList
from WMCore.Services.Dashboard.DashboardAPI import apmonSend, apmonFree
from WMCore.FwkJobReport.Report import Report

import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gErrorIgnoreLevel = ROOT.kError

from ROOT import TFile

fragment = """
import FWCore.ParameterSet.Config as cms
process.Timing = cms.Service("Timing",
    useJobReport = cms.untracked.bool(True),
    summaryOnly = cms.untracked.bool(True))

process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32({events}))

for prod in process.producers.values():
    if prod.hasParameter('nEvents') and prod.type_() == 'ExternalLHEProducer':
        prod.nEvents = cms.untracked.uint32({events})
"""

fragment_first = """
process.source.firstLuminosityBlock = cms.untracked.uint32({lumi})
"""

fragment_lumi = """
process.source.numberEventsInLuminosityBlock = cms.untracked.uint32({events})
"""

fragment_seeding = """
from IOMC.RandomEngine.RandomServiceHelper import RandomNumberServiceHelper
helper = RandomNumberServiceHelper(process.RandomNumberGeneratorService)
helper.populate()
"""

fragment_cores = """
if hasattr(process, 'options'):
    process.options.numberOfThreads = cms.untracked.uint32({cores})
else:
    process.options = cms.untracked.PSet(numberOfThreads = cms.untracked.uint32({cores}))
"""

sum_fragment = """
if hasattr(process, 'options'):
    process.options.wantSummary = cms.untracked.bool(True)
else:
    process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))
"""

runtime_fragment = """
process.maxSecondsUntilRampdown = cms.untracked.PSet(input = cms.untracked.int32({time}))
"""

monalisa = {
        'cms-jobmon.cern.ch:8884': {
            'sys_monitoring': 0,
            'general_info': 0,
            'job_monitoring': 0
        }
}

def run_subprocess(*args, **kwargs):
    print
    print ">>> executing"
    print " ".join(*args)

    retry = {}
    if 'retry' in kwargs:
        retry = dict(kwargs['retry'])
        del kwargs['retry']

    if 'stdout' not in kwargs.keys():
        kwargs['stdout'] = subprocess.PIPE
    if 'stderr' not in kwargs.keys():
        kwargs['stderr'] = subprocess.STDOUT
    p = subprocess.Popen(*args, **kwargs)
    out, err = p.communicate()

    if p.returncode in retry:
        print 'retrying...'
        if retry[p.returncode] > 0:
            retry[p.returncode] -= 1
            kwargs['retry'] = retry
            return run_subprocess(*args, **kwargs)

    # Set to the result of communicate, otherwise caller will not receive
    # any output.
    p.stdout = out
    p.stderr = err

    if p.stdout:
        print
        print ">>> result is"
        print p.stdout

    return p

def calculate_alder32(data):
    """Try to calculate checksums for output files.
    """

    for fn in data['files']['output info'].keys():
        checksum = '0'
        try:
            p = subprocess.Popen(['edmFileUtil', '-a', fn], stdout=subprocess.PIPE)
            stdout = p.communicate()[0]

            if p.returncode == 0:
                checksum = stdout.split()[-2]
                events = stdout.split()[-6]
        except:
            pass
        data['files']['output info'][fn]['adler32'] = checksum

@contextmanager
def check_execution(data, code):
    """Check execution within context.

    Updates 'task exit code' in `data`, if not already set, and prints a
    stack trace if the yield fails with an exception.
    """
    try:
        yield
    except:
        print traceback.format_exc()
        if data['task exit code'] == 0:
            data['task exit code'] = code

def check_output(config, localname, remotename):
    """Check that file has been transferred correctly.

    If XrootD or Chirp are in the output access methods, tries
    them in the order specified to compare the local and remote
    file sizes. If they agree, return True; otherwise, return False.
    """
    def compare(stat, file):
        # If there's no local file, there's nothing to compare
        if not os.path.isfile(file):
            return False

        size = os.path.getsize(file)
        match = re.search("[Ss]ize:\s*([0-9]*)", stat)
        if match:
            if int(size) == int(match.groups()[0]):
                return True
            else:
                print ">>> size mismatch after transfer"
                print "remote size: {0}".format(match.groups()[0])
                print "local size: {0}".format(size)
                return False
        else:
            return False

    for output in config['output']:
        if output.startswith('root://'):
            server, path = re.match("root://([a-zA-Z0-9:.\-]+)/(.*)", output).groups()
            timeout = '300' # if the server is bogus, xrdfs hangs instead of returning an error
            args = [
                "timeout",
                timeout,
                "xrdfs",
                server,
                "stat",
                os.path.join(path, remotename)
            ]
            p = run_subprocess(args, retry={53: 5})
            return compare(p.stdout, localname)
        elif output.startswith("chirp://"):
            server, path = re.match("chirp://([a-zA-Z0-9:.\-]+)/(.*)", output).groups()
            args = [
                os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp"),
                server,
                "stat",
                os.path.join(path, remotename)
            ]
            p = run_subprocess(args)
            return compare(p.stdout, localname)

    return True

def copy_inputs(data, config, env):
    """Copies input files if desired.

    Tries to access each input file via the specified access methods.
    Access methods are traversed in the order specified until one is successful.
    """
    if not config['mask']['files']:
        return

    config['file map'] = {}

    files = list(config['mask']['files'])
    config['mask']['files'] = []

    fast_track = False
    successes = defaultdict(int)

    for file in files:
        # If the file has been transferred by WQ, there's no need to
        # monkey around with the input list
        if os.path.exists(os.path.basename(file)):
            filename = 'file:' + os.path.basename(file)
            config['mask']['files'].append(filename)
            config['file map'][filename] = file

            print ">>> WQ transfer of input file detected:"
            print file
            continue

        # When the config specifies no "input," this implies to use
        # AAA to access data in, e.g., DBS
        if len(config['input']) == 0:
            config['mask']['files'].append(file)
            config['file map'][file] = file
            print ">>> AAA access to input file detected:"
            print file
            continue

        # Since we didn't find the file already here and we're not
        # using AAA, we need to go through the list of inputs and find
        # one that will allow us to access the file
        for input in config['input']:
            if input.startswith('file://'):
                path = os.path.join(input.replace('file://', '', 1), file)
                print ">>> Trying local access method:"
                if os.path.exists(path) and os.access(path, os.R_OK):
                    filename = 'file:' + path
                    config['mask']['files'].append(filename)
                    config['file map'][filename] = file

                    print ">>>> local access to input file detected:"
                    print path
                    break
                else:
                    print ">>>> local access to input file unavailable."
            elif input.startswith('root://'):
                print ">>> Trying xrootd access method:"
                server, path = re.match("root://([a-zA-Z0-9:.\-]+)/(.*)", input).groups()
                timeout = '300' # if the server is bogus, xrdfs hangs instead of returning an error
                args = [
                    "timeout",
                    timeout,
                    "xrdfs",
                    server,
                    "ls",
                    os.path.join(path, file)
                ]

                if fast_track or run_subprocess(args, retry={53: 5}).returncode == 0:
                    if config['disable streaming']:
                        print ">>>> streaming has been disabled, attempting stage-in"
                        args = [
                            "xrdcp",
                            os.path.join(input, file),
                            os.path.basename(file)
                        ]

                        p = run_subprocess(args)
                        if p.returncode == 0:
                            filename = 'file:' + os.path.basename(path)
                            config['mask']['files'].append(filename)
                            config['file map'][filename] = file
                            break
                    else:
                        print ">>>> will stream using xrootd instead of copying."
                        filename = os.path.join(input, file)
                        config['mask']['files'].append(filename)
                        config['file map'][filename] = file
                        break
                else:
                    print ">>>> xrootd access to input file unavailable."
            elif input.startswith('srm://'):
                print ">>> Trying srm access method:"
                prg = []
                if len(os.environ["LOBSTER_LCG_CP"]) > 0:
                    prg = [os.environ["LOBSTER_LCG_CP"], "-b", "-v", "-D", "srmv2"]
                elif len(os.environ["LOBSTER_GFAL_COPY"]) > 0:
                    # FIXME gfal is very picky about its environment
                    prg = [os.environ["LOBSTER_GFAL_COPY"]]

                args = prg + [
                    os.path.join(input, file),
                    os.path.basename(file)
                ]

                pruned_env = dict(env)
                for k in ['LD_LIBRARY_PATH', 'PATH']:
                    pruned_env[k] = ':'.join([x for x in os.environ[k].split(':') if 'CMSSW' not in x])

                p = run_subprocess(args, env=pruned_env)
                if p.returncode == 0:
                    print '>>>> Successfully copied input with SRM:'
                    filename = 'file:' + os.path.basename(file)
                    config['mask']['files'].append(filename)
                    config['file map'][filename] = file
                    break
                else:
                    print '>>>> Unable to copy input with SRM'
            elif input.startswith("chirp://"):
                print ">>> Trying chirp access method:"
                server, path = re.match("chirp://([a-zA-Z0-9:.\-]+)/(.*)", input).groups()
                remotename = os.path.join(path, file)

                args = [
                    os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_get"),
                    "-a",
                    "globus",
                    "-d",
                    "all",
                    server,
                    remotename,
                    os.path.basename(remotename)
                ]
                p = run_subprocess(args, env=env)
                if p.returncode == 0:
                    print '>>>> Successfully copied input with Chirp:'
                    filename = 'file:' + os.path.basename(file)
                    config['mask']['files'].append(filename)
                    config['file map'][filename] = file
                    break
                else:
                    print '>>>> Unable to copy input with Chirp'
            else:
                print '>>> skipping unhandled stage-in method: {0}'.format(input)
        else:
            print '>>> no stage out method succeeded for: {0}'.format(file)
            successes[input] -= 1
        successes[input] += 1

        if config.get('accelerate stage-in', 0) > 0 and not fast_track:
            method, count = max(successes.items(), key=lambda (x, y): y)
            if count > config['accelerate stage-in']:
                print ">>> Bypassing further access checks and using '{0}' for input".format(method)
                config['input'] = [method]
                fast_track = True

    if not config['mask']['files']:
        raise RuntimeError("no stage-in method succeeded")

    print ">>> modified input files:"
    for fn in config['mask']['files']:
        print fn

def copy_outputs(data, config, env):
    """Copy output files.

    If the task failed, delete output files, to avoid work_queue
    transferring them.  Otherwise, attempt stage-out methods in the order
    specified in the config['storage']['output'] section of the user's
    Lobster configuration. For successful tasks, file sizes are added up
    and inserted into the task data.
    """
    outsize = 0
    outsize_bare = 0

    transferred = []
    for localname, remotename in config['output files']:
        # prevent stageout of data for failed tasks
        if os.path.exists(localname) and data['exe exit code'] != 0:
            os.remove(localname)
            break
        elif data['exe exit code'] != 0:
            break

        outsize += os.path.getsize(localname)

        # using try just in case. Successful tasks should always
        # have an existing Events::TTree though.
        # Ha! Unless their output is not an EDM ROOT file, but
        # some other kind of file.  Good thing you used a try!
        try:
            outsize_bare += get_bare_size(localname)
        except IOError as error:
            print error
            print 'Could not calculate size as EDM ROOT file, try treating as regular file.'

            # Be careful here: getsize can thrown an exception!  No unhandled exceptions!
            try:
                outsize_bare += os.path.getsize(localname)
            except OSError as error:
                print error
                print 'Could not get size of output file {0} (may not exist).  Not adding its size.'

        for output in config['output']:
            if output.startswith('file://'):
                rn = os.path.join(output.replace('file://', ''), remotename)
                if os.path.isdir(os.path.dirname(rn)):
                    print ">>> local access detected"
                    print ">>> attempting stage-out with:"
                    print "shutil.copy2('{0}', '{1}')".format(localname, rn)
                    try:
                        shutil.copy2(localname, rn)
                        if check_output(config, localname, remotename):
                            transferred.append(localname)
                            break
                    except Exception as e:
                        print e
            elif output.startswith('srm://'):
                prg = []
                if len(os.environ["LOBSTER_LCG_CP"]) > 0:
                    prg = [os.environ["LOBSTER_LCG_CP"], "-b", "-v", "-D", "srmv2"]
                elif len(os.environ["LOBSTER_GFAL_COPY"]) > 0:
                    # FIXME gfal is very picky about its environment
                    prg = [os.environ["LOBSTER_GFAL_COPY"]]

                args = prg + [
                    "file://" + os.path.join(os.getcwd(), localname),
                    os.path.join(output, remotename)
                ]

                pruned_env = dict(env)
                for k in ['LD_LIBRARY_PATH', 'PATH']:
                    pruned_env[k] = ':'.join([x for x in os.environ[k].split(':') if 'CMSSW' not in x])

                ldpath = pruned_env.get('LD_LIBRARY_PATH', '')
                if ldpath != '':
                    ldpath += ':'
                ldpath += os.path.join(os.path.dirname(os.path.dirname(prg[0])), 'lib64')
                pruned_env['LD_LIBRARY_PATH'] = ldpath

                p = run_subprocess(args, env=pruned_env)
                if p.returncode == 0 and check_output(config, localname, remotename):
                    transferred.append(localname)
                    break
            elif output.startswith("chirp://"):
                server, path = re.match("chirp://([a-zA-Z0-9:.\-]+)/(.*)", output).groups()

                args = [os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_put"),
                        "-a",
                        "globus",
                        "-d",
                        "all",
                        localname,
                        server,
                        os.path.join(path, remotename)]
                p = run_subprocess(args, env=env)
                if p.returncode == 0 and check_output(config, localname, remotename):
                    transferred.append(localname)
                    break
            else:
                print '>>> skipping unhandled stage-out method: {0}'.format(output)

    if set([ln for ln, rn in config['output files']]) - set(transferred):
        raise RuntimeError("no stage-out method succeeded")

    data['output size'] = outsize
    data['output bare size'] = outsize_bare

def edit_process_source(pset, config):
    """Edit parameter set for task.

    Adjust input files and lumi mask, as well as adding a process summary
    for performance analysis.
    """
    files = config['mask']['files']
    lumis = LumiList(compactList=config['mask']['lumis']).getVLuminosityBlockRange()
    want_summary = config['want summary']
    runtime = config.get('task runtime')
    cores = config.get('cores')

    # MC production settings
    lumi_first = config['mask'].get('first lumi')
    lumi_events = config['mask'].get('events per lumi')
    seeding = config.get('randomize seeds', False)

    with open(pset, 'a') as fp:
        frag = fragment.format(events=config['mask']['events'])
        if any([f for f in files]):
            frag += "\nprocess.source.fileNames = cms.untracked.vstring({0})".format(repr([str(f) for f in files]))
        if lumis:
            frag += "\nprocess.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({0})".format([str(l) for l in lumis])
        if want_summary:
            frag += sum_fragment
        if runtime:
            frag += runtime_fragment.format(time=runtime)
        if seeding:
            frag += fragment_seeding
        if lumi_events:
            frag += fragment_lumi.format(events=lumi_events)
        if lumi_first:
            frag += fragment_first.format(lumi=lumi_first)
        if cores:
            frag += fragment_cores.format(cores=cores)

        print
        print ">>> config file fragment:"
        print frag
        fp.write(frag)

def parse_fwk_report(config, data, report_filename):
    """Extract task data from a framework report.

    Analyze the CMSSW job framework report to get the CMSSW exit code,
    skipped files, runs and lumis processed on a file basis, total events
    written, and CPU time overall and per event.
    """
    exit_code = 0
    skipped = []
    infos = {}
    written = 0
    eventsPerRun = 0

    report = Report("cmsrun")
    report.parse(report_filename)

    exit_code = report.getExitCode()

    for fn in report.getAllSkippedFiles():
        fn = config['file map'].get(fn, fn)
        skipped.append(fn)

    outinfos = {}
    for file in report.getAllFiles():
        pfn = file['pfn']
        outinfos[pfn] = {
                'runs': {},
                'events': file['events'],
        }
        written += int(file['events'])
        for run in file['runs']:
            try:
                outinfos[pfn]['runs'][run.run].extend(run.lumis)
            except KeyError:
                outinfos[pfn]['runs'][run.run] = run.lumis

    for file in report.getAllInputFiles():
        filename = file['lfn'] if len(file['lfn']) > 0 else file['pfn']
        filename = config['file map'].get(filename, filename)
        file_lumis = []
        try:
            for run in file['runs']:
                for lumi in run.lumis:
                    file_lumis.append((run.run, lumi))
        except AttributeError:
            print 'Detected file-based task.'
        infos[filename] = (int(file['events']), file_lumis)
        eventsPerRun += infos[filename][0]

    serialized = report.__to_json__(None)
    cputime = float(serialized['steps']['cmsrun']['performance']['cpu'].get('TotalJobCPU', '0'))

    data['files']['info'] = infos
    data['files']['output info'] = outinfos
    data['files']['skipped'] = skipped
    data['events written'] = written
    data['exe exit code'] = exit_code
    # For efficiency, we care only about the CPU time spent processing
    # events
    data['cpu time'] = cputime
    data['events per run'] = eventsPerRun

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

    def gettime(s):
        return re.search(r'[0-9]{1,2}-[A-Z][a-z]{2}-[0-9]{4} [0-9]{1,2}:[0-9]{2}:[0-9]{2}', s).group(0)

    with open(log_filename) as f:
        for line in f.readlines():
            if finit == default and re.search("Initiating request to open", line):
                finit = int(datetime.strptime(gettime(line), "%d-%b-%Y %X").strftime('%s'))
            elif fopen == default and re.search("Successfully opened", line):
                fopen = int(datetime.strptime(gettime(line), "%d-%b-%Y %X").strftime('%s'))
            elif first == default and re.search("the 1st record", line):
                first = int(datetime.strptime(gettime(line), "%d-%b-%Y %X").strftime('%s'))

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


configfile = sys.argv[1]
with open(configfile) as f:
    config = json.load(f)

cmsRun = True if config['executable'] == 'cmsRun' else False

data = {
    'files': {
        'info': {},
        'output info': {},
        'skipped': [],
    },
    'cache': {
        'start size': 0,
        'end size': 0,
        'type': None,
    },
    'task exit code': 0,
    '{0} exit code'.format('cmssw' if cmsRun else 'exe'): 0,
    'stageout exit code': 0,
    'cpu time': 0,
    'events written': 0,
    'output size': 0,
    'output bare size': 0,
    'task timing': {
        'stage in end': 0,
        'prologue end': 0,
        'wrapper start': 0,
        'wrapper ready': 0,
        'wrapper end': 0,
        'processing end': 0,
        'epilogue end': 0,
        'stage out end': 0,
    },
    'events per run': 0
}

env = os.environ
env['X509_USER_PROXY'] = 'proxy'


with check_execution(data, 179):
    copy_inputs(data, config, env)

data['task timing']['stage in end'] = int(datetime.now().strftime('%s'))

# Dashboard does not like Unicode, just ASCII encoding
monitorid = str(config['monitoring']['monitorid'])
syncid = str(config['monitoring']['syncid'])
taskid = str(config['monitoring']['taskid'])

args = config['arguments']

prologue = config.get('prologue', [])
epilogue = config.get('epilogue', [])

if prologue and len(prologue) > 0:
    print ">>> prologue:"
    with check_execution(data, 180):
        p = run_subprocess(prologue,env=env)

        # Was originally a subprocess.check_call, but this has the
        # potential to confuse log file output because print buffers
        # differently from the underlying process.  Therefore, do what
        # check_call would do and raise a CalledProcessError if we get
        # a non-zero return code.
        if p.returncode != 0:
            raise subprocess.CalledProcessError


data['task timing']['prologue end'] = int(datetime.now().strftime('%s'))

parameters = {
            'ExeStart': str(config['executable']),
            'NCores': config.get('cores', 1),
            'SyncCE': 'ndcms.crc.nd.edu',
            'SyncGridJobId': syncid,
            'WNHostName': os.environ.get('HOSTNAME', '')
            }

apmonSend(taskid, monitorid, parameters, logging, monalisa)
apmonFree()

print
print ">>> updated parameters are:"
print json.dumps(config, sort_keys=True, indent=2)

if cmsRun:
    #
    # Start proper CMSSW job
    #

    pset = config['pset']
    pset_mod = pset.replace(".py", "_mod.py")
    shutil.copy2(pset, pset_mod)

    edit_process_source(pset_mod, config)

    cmd = ['cmsRun', '-j', 'report.xml', pset_mod]
    cmd.extend([str(arg) for arg in args])
else:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    cmd = config['executable']
    if isinstance(cmd, basestring):
        cmd = shlex.split(cmd)
    if os.path.isfile(cmd[0]):
        cmd[0] = os.path.join(os.getcwd(), cmd[0])
    cmd.extend([str(arg) for arg in args])

    if config.get('append inputs to args', False):
        cmd.extend([str(f) for f in config['mask']['files']])

print ">>> running {0}".format(' '.join(cmd))
# Open a file handle for the executable log
with open('executable.log', 'w') as logfile:
    p = run_subprocess(cmd, stdout=logfile, stderr=subprocess.STDOUT, env=env)
data['exe exit code'] = p.returncode
data['task exit code'] = data['exe exit code']

if p.returncode != 0:
    print ">>> Executable returned non-zero exit code {0}.".format(p.returncode)

if cmsRun:
    apmonSend(taskid, monitorid, {'ExeEnd': 'cmsRun', 'NCores': config.get('cores', 1)}, logging, monalisa)

    with check_execution(data, 190):
        parse_fwk_report(config, data, 'report.xml')

    calculate_alder32(data)
else:
    data['files']['info'] = dict((f, [0, []]) for f in config['file map'].values())
    data['files']['output info'] = dict((f, {'runs': {}, 'events': 0, 'adler32': '0'}) for f, rf in config['output files'])
    data['cpu time'] = usage.ru_stime

with check_execution(data, 191):
    data['task timing']['wrapper start'] = extract_time('t_wrapper_start')
    data['task timing']['wrapper ready'] = extract_time('t_wrapper_ready')

now = int(datetime.now().strftime('%s'))
data['task timing']['processing end'] = now

if epilogue and len(epilogue) > 0:
    # Make data collected so far available to the epilogue
    with open('report.json', 'w') as f:
        json.dump(data, f, indent=2)
    print ">>> epilogue:"
    with check_execution(data, 199):
        p = run_subprocess(epilogue, env=env)

        # Was originally a subprocess.check_call, but this has the
        # potential to confuse log file output because print buffers
        # differently from the underlying process.  Therefore, do what
        # check_call would do and raise a CalledProcessError if we get
        # a non-zero return code.
        if p.returncode != 0:
            raise subprocess.CalledProcessError
    with open('report.json', 'r') as f:
        data = json.load(f)

data['task timing']['epilogue end'] = int(datetime.now().strftime('%s'))

with check_execution(data, 210):
    copy_outputs(data, config, env)
# Also set stageout exit code if copy_outputs fails
if data['task exit code'] == 210:
    data['stageout exit code'] = 210

transfer_success = all(check_output(config, local, remote) for local, remote in config['output files'])
if data['task exit code'] == 0 and not transfer_success:
    data['task exit code'] = 211
    data['stageout exit code'] = 211
    data['output size'] = 0

data['task timing']['stage out end'] = int(datetime.now().strftime('%s'))

if 'PARROT_ENABLED' in os.environ:
    cachefile = os.path.join(os.environ['PARROT_CACHE'], 'hot_cache')
    if not os.path.isfile(cachefile):
        # Write the time of the first event to the cache file.  At that
        # point in processing, almost everything should have been pulled
        # from CVMFS.
        with open(cachefile, 'w') as f:
            f.write(str(data['task timing']['processing end']))
        data['cache']['type'] = 0
    else:
        with open(cachefile) as f:
            fullcache = int(f.read())
            selfstart = extract_time('t_wrapper_start')

            # If our wrapper started before the cache was filled, we are
            # still a cold cache task (value 0.)  Otherwise, we were
            # operating on a hot cache.
            data['cache']['type'] = int(selfstart > fullcache)
else:
    data['cache']['type'] = 2

with check_execution(data, 193):
    with open('report.json', 'w') as f:
        json.dump(data, f, indent=2)

for filename in 'executable.log report.xml'.split():
    if os.path.isfile(filename):
        with check_execution(data, 194):
            with open(filename) as f:
                zipf = gzip.open(filename + ".gz", "wb")
                zipf.writelines(f)
                zipf.close()

cputime = data['cpu time']
total_time = data['task timing']['stage out end'] - data['task timing']['wrapper start']
exe_time = data['task timing']['stage out end'] - data['task timing']['prologue end']
task_exit_code = data['task exit code']
stageout_exit_code = data['stageout exit code']
events_per_run = data['events per run']
exe_exit_code = data['{0} exit code'.format('cmssw' if cmsRun else 'exe')]

print "Execution time", str(total_time)

print "Exiting with code", str(task_exit_code)
print "Reporting ExeExitCode", str(exe_exit_code)
print "Reporting StageOutExitCode", str(stageout_exit_code)

parameters = {
            'ExeTime': str(exe_time),
            'ExeExitCode': str(exe_exit_code),
            'JobExitCode': str(task_exit_code),
            'JobExitReason': '',
            'StageOutSE': 'ndcms.crc.nd.edu',
            'StageOutExitStatus': str(stageout_exit_code),
            'StageOutExitStatusReason': 'Copy succedeed with srm-lcg utils',
            'CrabUserCpuTime': str(cputime),
            'CrabWrapperTime': str(total_time),
            'WCCPU': str(total_time),
            'NoEventsPerRun': str(events_per_run),
            'NbEvPerRun': str(events_per_run),
            'NEventsProcessed': str(events_per_run)
            }
try:
    parameters.update({'CrabCpuPercentage': str(float(cputime)/float(total_time))})
except:
    pass

apmonSend(taskid, monitorid, parameters, logging, monalisa)
apmonFree()

sys.exit(task_exit_code)
