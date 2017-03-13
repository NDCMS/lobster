#!/usr/bin/env python

from collections import defaultdict, Counter
from contextlib import contextmanager
from datetime import datetime
import atexit
import gzip
import json
import logging
import os
import re
import resource
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import traceback

sys.path.append('python')

from WMCore.DataStructs.LumiList import LumiList
from WMCore.FwkJobReport.Report import Report
from WMCore.Services.Dashboard.DashboardAPI import DashboardAPI
from WMCore.Storage.SiteLocalConfig import loadSiteLocalConfig

import ROOT

ROOT.gROOT.SetBatch(True)
ROOT.PyConfig.IgnoreCommandLineOptions = True
ROOT.gErrorIgnoreLevel = ROOT.kError

from ROOT import TFile


class Dash(object):

    def __init__(self):
        self.__api = DashboardAPI(logr=logging.getLogger('mona'))

    def configure(self, config):
        self.__jobid = str(config['monitoring']['monitorid'])
        self.__taskid = str(config['monitoring']['taskid'])

    def __call__(self, params):
        with self.__api as dashboard:
            params['taskid'] = self.__taskid
            params['jobid'] = self.__jobid
            dashboard.apMonSend(params)


publish = Dash()


class Mangler(logging.Formatter):

    def __init__(self):
        super(Mangler, self).__init__(fmt='%(message)s')
        self.context = None

    @contextmanager
    def output(self, context):
        old, self.context = self.context, context
        yield
        self.context = old

    def format(self, record):
        if record.levelno >= logging.INFO:
            fmt = '{chevron} {message} @ {date}'
        elif record.levelno == logging.DEBUG and self.context:
            fmt = '{chevron} {context}: {message}'
        else:
            fmt = '{chevron} {message}'
        chevron = '>' * (record.levelno / logging.DEBUG + 1)
        return fmt.format(chevron=chevron, message=record.msg, date=time.strftime("%c"), context=self.context)


mangler = Mangler()

console = logging.StreamHandler()
console.setFormatter(mangler)

logger = logging.getLogger('prawn')
logger.addHandler(console)
logger.propagate = False
logger.setLevel(logging.DEBUG)

fragment = """
import FWCore.ParameterSet.Config as cms
process.Timing = cms.Service("Timing",
    useJobReport = cms.untracked.bool(True),
    summaryOnly = cms.untracked.bool(True))

process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32({events}))

import os
_, major, minor, _ = os.environ["CMSSW_VERSION"].split('_', 3)
if int(major) >= 7 and int(minor) >= 4:
    xrdstats = cms.Service("XrdAdaptor::XrdStatisticsService",  cms.untracked.PSet(reportToFJR = cms.untracked.bool(True)))
    process.add_(xrdstats)

for prod in process.producers.values():
    if prod.hasParameter('nEvents') and prod.type_() == 'ExternalLHEProducer':
        prod.nEvents = cms.untracked.uint32({events})
"""

fragment_first_run = """
process.source.firstRun = cms.untracked.uint32({run})
"""

fragment_first_lumi = """
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
    process.options.numberOfStreams = cms.untracked.uint32(0)
else:
    process.options = cms.untracked.PSet(
        numberOfThreads = cms.untracked.uint32({cores}),
        numberOfStreams = cms.untracked.uint32(0)
    )
"""

fragment_sum = """
if hasattr(process, 'options'):
    process.options.wantSummary = cms.untracked.bool(True)
else:
    process.options = cms.untracked.PSet(wantSummary = cms.untracked.bool(True))
"""

fragment_runtime = """
process.maxSecondsUntilRampdown = cms.untracked.PSet(input = cms.untracked.int32({time}))
"""

fragment_gridpack = """
for prod in process.producers.values():
    if prod.hasParameter('args') and prod.type_() == 'ExternalLHEProducer':
        prod.args = cms.vstring('{gridpack}')
"""


def run_subprocess(*args, **kwargs):
    logger.info("executing '{}'".format(" ".join(*args)))

    retry = kwargs.pop('retry', {})
    capture = kwargs.pop('capture', False)

    outfd, outfn = tempfile.mkstemp()

    logger.debug("using {} to store command output".format(outfn))

    with open(outfn, 'wb') as out:
        kwargs['stdout'] = out
        kwargs['stderr'] = subprocess.STDOUT
        p = subprocess.Popen(*args, **kwargs)

    _, _ = p.communicate()

    p.stdout = ""
    with open(outfn, 'r') as fd:
        with mangler.output('cmd'):
            for line in fd:
                logger.debug(line.strip())
                if capture:
                    p.stdout += line
    os.unlink(outfn)

    if p.returncode in retry:
        logger.info("retrying command")
        if retry[p.returncode] > 0:
            retry[p.returncode] -= 1
            kwargs['retry'] = retry
            return run_subprocess(*args, **kwargs)

    return p


def calculate_alder32(data):
    """Try to calculate checksums for output files.
    """

    for fn in data['files']['output_info'].keys():
        checksum = '0'
        try:
            p = subprocess.Popen(['edmFileUtil', '-a', fn], stdout=subprocess.PIPE)
            stdout = p.communicate()[0]

            if p.returncode == 0:
                checksum = stdout.split()[-2]
        except Exception:
            pass
        data['files']['output_info'][fn]['adler32'] = checksum


def check_execution(exitcode, update=None, timing=None):
    """Decorator to quit upon exception.

    Execute the wrapper function, and, in case it throws an exception, set
    task_exit_code and update the first argument of the wrapped
    function with the optional update passed to the decorator.

    The first argument of the wrapped function **must** be a dictionary to
    contain information about the Lobster task.

    Parameters
    ----------
    exitcode : int
        The exit code with which the wrapper should quit when the decorated
        function raises an exception.
    update : dict
        Update the dictionary passed to the decorated function as a first
        parameter with this dictionary when the function raises an
        exception.
    timing : str
        Update the dictionary passed to the decorated function by adding a
        key `timing` with the seconds since UNIX epoch to the `task_timing`
        sub-dictionary.
    """
    if update is None:
        update = {}

    def decorator(fct):
        def wrapper(data, *args, **kwargs):
            ecode = kwargs.pop('exitcode', exitcode)
            try:
                result = fct(data, *args, **kwargs)
            except Exception:
                with mangler.output('trace'):
                    for l in traceback.format_exc().splitlines():
                        logger.debug(l)
                data['task_exit_code'] = ecode
                data.update(update)
                logger.error("call to '{}' failed, exiting with exit code {}".format(fct.func_name, ecode))
                sys.exit(ecode)
            finally:
                if timing:
                    data['task_timing'][timing] = int(datetime.now().strftime('%s'))
            return result
        return wrapper
    return decorator


def check_output(config, localname, remotename):
    """Check that file has been transferred correctly.

    If file, XrootD or Chirp are in the output access methods, tries
    them in the order specified to compare the local and remote
    file sizes. If they agree, return True; otherwise, return False.

    If file, XrootD, or Chirp are not in the output access methods,
    return True.
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
                logger.error("size mismatch after transfer")
                logger.debug("remote size: {0}".format(match.groups()[0]))
                logger.debug("local size: {0}".format(size))
                return False
        else:
            raise RuntimeError('checking output for {0} failed: {1}'.format(file, stat))

    for output in config['output']:
        if output.startswith('file://'):
            path = output.replace('file://', '')
            args = [
                "stat",
                os.path.join(path, remotename)
            ]
            p = run_subprocess(args, retry={53: 5}, capture=True)
            try:
                return compare(p.stdout, localname)
            except RuntimeError as e:
                logger.error(e)
        if output.startswith('root://'):
            server, path = re.match("root://([a-zA-Z0-9:.\-]+)/(.*)", output).groups()
            timeout = '300'  # if the server is bogus, xrdfs hangs instead of returning an error
            args = [
                "timeout",
                timeout,
                "xrdfs",
                server,
                "stat",
                os.path.join(path, remotename)
            ]
            p = run_subprocess(args, retry={53: 5}, capture=True)
            try:
                return compare(p.stdout, localname)
            except RuntimeError as e:
                logger.error(e)
        elif output.startswith("chirp://"):
            server, path = re.match("chirp://([a-zA-Z0-9:.\-]+)/(.*)", output).groups()
            args = [
                os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp"),
                "--timeout",
                "900",
                server,
                "stat",
                os.path.join(path, remotename)
            ]
            p = run_subprocess(args, capture=True)
            try:
                return compare(p.stdout, localname)
            except RuntimeError as e:
                logger.error(e)

    return True


@check_execution(exitcode=211, update={'stageout_exit_code': 211, 'output_size': 0}, timing='stage_out_end')
def check_outputs(data, config):
    for local, remote in config['output files']:
        if not check_output(config, local, remote):
            raise IOError("could not verify output file '{}'".format(remote))


def check_parrot_cache(data):
    if 'PARROT_ENABLED' in os.environ:
        cachefile = os.path.join(os.environ['PARROT_CACHE'], 'hot_cache')
        if not os.path.isfile(cachefile):
            # Write the time of the first event to the cache file.  At that
            # point in processing, almost everything should have been pulled
            # from CVMFS.
            with open(cachefile, 'w') as f:
                f.write(str(data['task_timing']['processing_end']))
            data['cache']['type'] = 0
        else:
            with open(cachefile) as f:
                try:
                    fullcache = int(f.read())
                    selfstart = data['task_timing']['wrapper_start']

                    # If our wrapper started before the cache was filled, we are
                    # still a cold cache task (value 0.)  Otherwise, we were
                    # operating on a hot cache.
                    data['cache']['type'] = int(selfstart > fullcache)
                except ValueError:
                    data['cache']['type'] = 0


@check_execution(exitcode=179, timing='stage_in_end')
def copy_inputs(data, config, env):
    """Copies input files if desired.

    Tries to access each input file via the specified access methods.
    Access methods are traversed in the order specified until one is successful.
    """
    config['file map'] = {}

    if not config['mask']['files']:
        return

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

            logger.info("WQ transfer of input file {} detected".format(file))
            data['transfers']['wq']['stage-in success'] += 1
            continue

        # When the config specifies no "input," this implies to use
        # AAA to access data in, e.g., DBS
        if len(config['input']) == 0:
            config['mask']['files'].append(file)
            config['file map'][file] = file
            logger.info("AAA access to input file {} detected".format(file))
            data['transfers']['root']['stage-in success'] += 1
            continue

        # Since we didn't find the file already here and we're not
        # using AAA, we need to go through the list of inputs and find
        # one that will allow us to access the file
        for input in config['input']:
            if input.startswith('file://'):
                path = os.path.join(input.replace('file://', '', 1), file)
                logger.info("Trying local access method")
                if os.path.exists(path) and os.access(path, os.R_OK):
                    filename = 'file:' + path
                    config['mask']['files'].append(filename)
                    config['file map'][filename] = file

                    logger.info("Local access to input file {} detected".format(path))
                    data['transfers']['file']['stage-in success'] += 1
                    break
                else:
                    logger.info("Local access to input file unavailable")
                    data['transfers']['file']['stage-in failure'] += 1
            elif input.startswith('root://'):
                logger.info("Trying xrootd access method")
                server, path = re.match("root://([a-zA-Z0-9:.\-]+)/(.*)", input).groups()
                timeout = '300'  # if the server is bogus, xrdfs hangs instead of returning an error
                args = [
                    "env",
                    "XRD_LOGLEVEL=Debug",
                    "timeout",
                    timeout,
                    "xrdfs",
                    server,
                    "stat",
                    os.path.join(path, file)
                ]

                if fast_track or run_subprocess(args, retry={53: 5}).returncode == 0:
                    if config['disable streaming']:
                        logger.info("streaming has been disabled, attempting stage-in")
                        args = [
                            "env",
                            "XRD_LOGLEVEL=Debug",
                            "xrdcp",
                            os.path.join(input, file.lstrip('/')),
                            os.path.basename(file)
                        ]

                        p = run_subprocess(args)
                        if p.returncode == 0:
                            filename = 'file:' + os.path.basename(file)
                            config['mask']['files'].append(filename)
                            config['file map'][filename] = file
                            data['transfers']['xrdcp']['stage-in success'] += 1
                            break
                        else:
                            data['transfers']['xrdcp']['stage-in failure'] += 1
                    else:
                        logger.info("will stream using xrootd instead of copying")
                        filename = os.path.join(input, file)
                        config['mask']['files'].append(filename)
                        config['file map'][filename] = file
                        data['transfers']['root']['stage-in success'] += 1
                        break
                else:
                    logger.info("xrootd access to input file unavailable")
            elif input.startswith('srm://') or input.startswith('gsiftp://'):
                logger.info("Trying srm access method")
                prg = []
                if len(os.environ["LOBSTER_LCG_CP"]) > 0 and not input.startswith('gsiftp://'):
                    prg = [os.environ["LOBSTER_LCG_CP"], "-b", "-v", "-D", "srmv2", "--sendreceive-timeout", "600"]
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
                    logger.info('Successfully copied input with SRM')
                    filename = 'file:' + os.path.basename(file)
                    config['mask']['files'].append(filename)
                    config['file map'][filename] = file
                    data['transfers']['srm']['stage-in success'] += 1
                    break
                else:
                    logger.error('Unable to copy input with SRM')
                    data['transfers']['srm']['stage-in failure'] += 1
            elif input.startswith("chirp://"):
                logger.info("Trying chirp access method")
                server, path = re.match("chirp://([a-zA-Z0-9:.\-]+)/(.*)", input).groups()
                remotename = os.path.join(path, file)

                args = [
                    os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_get"),
                    "-a",
                    "globus",
                    "-d",
                    "all",
                    "--timeout",
                    "900",
                    server,
                    remotename,
                    os.path.basename(remotename)
                ]
                p = run_subprocess(args, env=env)
                if p.returncode == 0:
                    logger.info('Successfully copied input with Chirp')
                    filename = 'file:' + os.path.basename(file)
                    config['mask']['files'].append(filename)
                    config['file map'][filename] = file
                    data['transfers']['chirp']['stage-in success'] += 1
                    break
                else:
                    logger.error('Unable to copy input with Chirp')
                    data['transfers']['chirp']['stage-in failure'] += 1
            else:
                logger.warning('skipping unhandled stage-in method: {0}'.format(input))
        else:
            logger.critical('no stage out method succeeded for: {0}'.format(file))
            successes[input] -= 1
        successes[input] += 1

        if config.get('accelerate stage-in', 0) > 0 and not fast_track:
            method, count = max(successes.items(), key=lambda (x, y): y)
            if count > config['accelerate stage-in']:
                logger.info("Bypassing further access checks and using '{0}' for input".format(method))
                config['input'] = [method]
                fast_track = True

    if not config['mask']['files']:
        raise RuntimeError("no stage-in method succeeded")

    logger.info("modified input files")
    with mangler.output('input'):
        for fn in config['mask']['files']:
            logger.debug(fn)


@check_execution(exitcode=210, update={'stageout_exit_code': 210}, timing='stage_out_end')
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

    server_re = re.compile("[a-zA-Z]+://([a-zA-Z0-9:.\-]+)/")
    target_se = []
    default_se = config['default se']

    transferred = []
    for localname, remotename in config['output files']:
        # prevent stageout of data for failed tasks
        if os.path.exists(localname) and data['exe_exit_code'] != 0:
            os.remove(localname)
            break
        elif data['exe_exit_code'] != 0:
            break

        outsize += os.path.getsize(localname)

        try:
            outsize_bare += get_bare_size(localname)
        except IOError:
            logger.warning('detected non-EDM output; using filesystem-reported file size for merge calculation')
            try:
                outsize_bare += os.path.getsize(localname)
            except OSError as e:
                logger.error('missing output file {}: {}'.format(localname, e))
            except Exception as e:
                logger.error("file size detection for {} failed with: {}".format(localname, e))

        for output in config['output']:
            if output.startswith('file://'):
                rn = os.path.join(output.replace('file://', ''), remotename)
                if os.path.isdir(os.path.dirname(rn)):
                    logger.info("local access detected")
                    logger.info("attempting stage-out with `shutil.copy2('{0}', '{1}')`".format(localname, rn))
                    try:
                        shutil.copy2(localname, rn)
                        if check_output(config, localname, remotename):
                            transferred.append(localname)
                            target_se.append(default_se)
                            data['transfers']['file']['stageout success'] += 1
                            break
                    except Exception as e:
                        logger.critical(e)
                        data['transfers']['file']['stageout failure'] += 1
            elif output.startswith('srm://') or output.startswith('gsiftp://'):
                protocol = output[:output.find(':')]
                prg = []
                if len(os.environ["LOBSTER_LCG_CP"]) > 0 and output.startswith('srm://'):
                    prg = [os.environ["LOBSTER_LCG_CP"], "-b", "-v", "-D", "srmv2", "--sendreceive-timeout", "600"]
                elif len(os.environ["LOBSTER_GFAL_COPY"]) > 0:
                    # FIXME gfal is very picky about its environment
                    prg = [os.environ["LOBSTER_GFAL_COPY"]]
                else:
                    data['transfers'][protocol]['stageout failure'] += 1
                    continue

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
                    match = server_re.match(args[-1])
                    if match:
                        target_se.append(match.group(1))
                    data['transfers'][protocol]['stageout success'] += 1
                    break
                else:
                    data['transfers'][protocol]['failure'] += 1
            elif output.startswith("chirp://"):
                server, path = re.match("chirp://([a-zA-Z0-9:.\-]+)/(.*)", output).groups()

                args = [os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_put"),
                        "-a",
                        "globus",
                        "-d",
                        "all",
                        "--timeout",
                        "900",
                        localname,
                        server,
                        os.path.join(path, remotename)]
                p = run_subprocess(args, env=env)
                if p.returncode == 0 and check_output(config, localname, remotename):
                    transferred.append(localname)
                    match = server_re.match(args[-1])
                    if match:
                        target_se.append(match.group(1))
                    data['transfers']['chirp']['stageout success'] += 1
                    break
                else:
                    data['transfers']['chirp']['stageout failure'] += 1
            else:
                logger.warning('skipping unhandled stage-out method: {0}'.format(output))

    if set([ln for ln, _ in config['output files']]) - set(transferred):
        raise RuntimeError("no stage-out method succeeded")

    data['output_size'] = outsize
    data['output_bare_size'] = outsize_bare
    data['output_storage_element'] = default_se

    if len(target_se) > 0:
        data['output storager element'] = max(((se, target_se.count(se)) for se in set(target_se)), key=lambda (x, y): y)[0]


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
    run_first = config['mask'].get('first run')
    lumi_first = config['mask'].get('first lumi')
    lumi_events = config['mask'].get('events per lumi')
    seeding = config.get('randomize seeds', False)

    with open(pset, 'a') as fp:
        frag = fragment.format(events=config['mask']['events'])
        if any([f for f in files]) and not config['gridpack']:
            frag += "\nprocess.source.fileNames = cms.untracked.vstring({0})".format(repr([str(f) for f in files]))
        if config['gridpack']:
            # ExternalLHEProducer only understands local files and does
            # not expect the `file:` prefix. Also, there can never be
            # more than one gridpack, so take the first element.
            frag += fragment_gridpack.format(gridpack=os.path.abspath(files[0].replace('file:', '')))
        if lumis:
            frag += "\nprocess.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({0})".format([str(l) for l in lumis])
        if want_summary:
            frag += fragment_sum
        if runtime:
            frag += fragment_runtime.format(time=runtime)
        if seeding:
            frag += fragment_seeding
        if lumi_events:
            frag += fragment_lumi.format(events=lumi_events)
        if lumi_first:
            frag += fragment_first_lumi.format(lumi=lumi_first)
        if run_first:
            frag += fragment_first_run.format(run=run_first)
        if cores:
            frag += fragment_cores.format(cores=cores)

        logger.info("config file fragment")
        with mangler.output('pset'):
            for l in frag.splitlines():
                logger.debug(l)
        fp.write(frag)


@check_execution(exitcode=190)
def parse_fwk_report(data, config, report_filename):
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
            logger.info('Detected file-based task')
        infos[filename] = (int(file['events']), file_lumis)
        eventsPerRun += infos[filename][0]

    serialized = report.__to_json__(None)
    cputime = float(serialized['steps']['cmsrun']['performance']['cpu'].get('TotalJobCPU', '0'))

    data['files']['info'] = infos
    data['files']['output_info'] = outinfos
    data['files']['skipped'] = skipped
    data['events_written'] = written
    data['exe_exit_code'] = exit_code
    # For efficiency, we care only about the CPU time spent processing
    # events
    data['cpu_time'] = cputime
    data['events_per_run'] = eventsPerRun


@check_execution(exitcode=191)
def extract_wrapper_times(data):
    """Load file contents as integer timestamp.
    """
    for key, filename in [
            ('wrapper_start', 't_wrapper_start'),
            ('wrapper_ready', 't_wrapper_ready')]:
        with open(filename) as f:
            data['task_timing'][key] = int(f.readline())


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
    """Get the output_bare_size.

    This size does not include space taken up by headers and other
    metadata in EDM files. It is needed to predict how many input
    files will result in a merged output file of a given size.

    Extracts Events->TTree::GetZipBytes()
    """
    rootfile = TFile(filename, "READ")
    if rootfile.IsZombie():
        raise IOError("Can't open ROOT file '{0}'".format(filename))

    size = 0
    for treename in ("Events", "Runs", "Lumis"):
        if not rootfile.GetListOfKeys().Contains(treename):
            rootfile.Close()
            raise IOError("Can't find tree '{1}' in  ROOT file '{0}'".format(filename, treename))
        size += rootfile.Get(treename).GetZipBytes()
    rootfile.Close()
    return size


@check_execution(exitcode=185, timing='processing_end')
def run_command(data, config, env):
    cmd = config['executable']
    args = config['arguments']
    if 'cmsRun' in cmd:
        pset = config['pset']
        pset_mod = pset.replace(".py", "_mod.py")
        shutil.copy2(pset, pset_mod)

        edit_process_source(pset_mod, config)

        cmd = [cmd, '-j', 'report.xml', pset_mod]
        cmd.extend([str(arg) for arg in args])
    else:
        usage = resource.getrusage(resource.RUSAGE_CHILDREN)
        if isinstance(cmd, basestring):
            cmd = shlex.split(cmd)
        if os.path.isfile(cmd[0]):
            cmd[0] = os.path.join(os.getcwd(), cmd[0])
        cmd.extend([str(arg) for arg in args])

        if config.get('append inputs to args', False):
            cmd.extend([str(f) for f in config['mask']['files']])

    p = run_subprocess(cmd, env=env)
    logger.info("executable returned with exit code {0}.".format(p.returncode))
    data['exe_exit_code'] = p.returncode
    data['task_exit_code'] = data['exe_exit_code']

    publish({'ExeEnd': config['executable'], 'NCores': config.get('cores', 1)})

    if 'cmsRun' in config['executable']:
        if p.returncode == 0:
            parse_fwk_report(data, config, 'report.xml')
            calculate_alder32(data)
        else:
            parse_fwk_report(data, config, 'report.xml', exitcode=p.returncode)
    else:
        data['files']['info'] = dict((f, [0, []]) for f in config['file map'].values())
        data['files']['output_info'] = dict((f, {'runs': {}, 'events': 0, 'adler32': '0'}) for f, rf in config['output files'])
        data['cpu_time'] = usage.ru_stime

    if p.returncode != 0:
        raise subprocess.CalledProcessError


def run_step(data, config, env, name):
    step = config.get(name, [])
    if step and len(step) > 0:
        logger.info(name)
        p = run_subprocess(step, env=env)
        # Was originally a subprocess.check_call, but this has the
        # potential to confuse log file output because print buffers
        # differently from the underlying process.  Therefore, do what
        # check_call would do and raise a CalledProcessError if we get
        # a non-zero return code.
        if p.returncode != 0:
            raise subprocess.CalledProcessError


@check_execution(exitcode=180, timing='prologue_end')
def run_prologue(data, config, env):
    run_step(data, config, env, 'prologue')


@check_execution(exitcode=199, timing='epilogue_end')
def run_epilogue(data, config, env):
    with open('report.json', 'w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')
    run_step(data, config, env, 'epilogue')
    with open('report.json', 'r') as f:
        update = json.load(f)
        # Update data in memory without changing the reference
        for k in update.keys():
            # List of allowed keys to update: currently only file metadata
            if k not in ('files',):
                continue
            elif k not in data:
                del data[k]
            else:
                data[k] = update[k]
        # Dumping `data` turns the defaultdict of Counters into a dict of
        # dicts, so copy it back into a defaultdict of Counters
        transfers = defaultdict(Counter)
        for protocol in data['transfers']:
            transfers[protocol].update(data['transfers'][protocol])
        data['transfers'] = transfers


def send_initial_dashboard_update(data, config):
    # Dashboard does not like Unicode, just ASCII encoding
    syncid = str(config['monitoring']['syncid'])

    try:
        if os.environ.get("PARROT_ENABLED", "FALSE") == "TRUE":
            raise ValueError()
        sync_ce = loadSiteLocalConfig().siteName
    except Exception:
        for envvar in ["GLIDEIN_Gatekeeper", "OSG_HOSTNAME", "CONDORCE_COLLECTOR_HOST"]:
            if envvar in os.environ:
                sync_ce = os.environ[envvar]
                break
        else:
            host = socket.getfqdn()
            sync_ce = config['default host']
            if host.rsplit('.')[-2:] == sync_ce.rsplit('.')[-2:]:
                sync_ce = config['default ce']
            else:
                sync_ce = 'Unknown'

    logger.info("using sync CE {}".format(sync_ce))

    parameters = {
        'ExeStart': str(config['executable']),
        'NCores': config.get('cores', 1),
        'SyncCE': sync_ce,
        'SyncGridJobId': syncid,
        'WNHostName': socket.getfqdn()
    }
    publish(parameters)


def send_final_dashboard_update(data, config):
    cputime = data['cpu_time']
    events_per_run = data['events_per_run']
    exe_exit_code = data['exe_exit_code']
    exe_time = data['task_timing']['stage_out_end'] - data['task_timing']['prologue_end']
    task_exit_code = data['task_exit_code']
    total_time = data['task_timing']['stage_out_end'] - data['task_timing']['wrapper_start']
    stageout_exit_code = data['stageout_exit_code']
    stageout_se = data['output_storage_element']

    logger.debug("Execution time {}".format(total_time))
    logger.debug("Exiting with code {}".format(task_exit_code))
    logger.debug("Reporting ExeExitCode {}".format(exe_exit_code))
    logger.debug("Reporting StageOutSE {}".format(stageout_se))
    logger.debug("Reporting StageOutExitCode {}".format(stageout_exit_code))

    parameters = {
        'ExeTime': str(exe_time),
        'ExeExitCode': str(exe_exit_code),
        'JobExitCode': str(task_exit_code),
        'JobExitReason': '',
        'StageOutSE': stageout_se,
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
        parameters.update({'CrabCpuPercentage': str(float(cputime) / float(total_time))})
    except Exception:
        pass

    publish(parameters)


def write_report(data):
    with open('report.json', 'w') as f:
        json.dump(data, f, indent=2)
        f.write('\n')


def write_zipfiles(data):
    filename = 'report.xml'
    if os.path.exists(filename):
        with open(filename) as f:
            zipf = gzip.open(filename + ".gz", "wb")
            zipf.writelines(f)
            zipf.close()


data = {
    'files': {
        'info': {},
        'output_info': {},
        'skipped': [],
    },
    'cache': {
        'start_size': 0,
        'end_size': 0,
        'type': 2,
    },
    'task_exit_code': 0,
    'exe_exit_code': 0,
    'stageout_exit_code': 0,
    'cpu_time': 0,
    'events_written': 0,
    'output_size': 0,
    'output_bare_size': 0,
    'output_storage_element': '',
    'task_timing': {
        'stage_in_end': 0,
        'prologue_end': 0,
        'wrapper_start': 0,
        'wrapper_ready': 0,
        'processing_end': 0,
        'epilogue_end': 0,
        'stage_out_end': 0,
    },
    'events_per_run': 0,
    'transfers': defaultdict(Counter)
}

configfile = sys.argv[1]
with open(configfile) as f:
    config = json.load(f)

publish.configure(config)

atexit.register(send_final_dashboard_update, data, config)
atexit.register(write_report, data)
atexit.register(write_zipfiles, data)

logger.info('data is {0}'.format(str(data)))
env = os.environ
env['X509_USER_PROXY'] = 'proxy'

extract_wrapper_times(data)
copy_inputs(data, config, env)

logger.info("updated parameters are")
with mangler.output("json"):
    for l in json.dumps(config, sort_keys=True, indent=2).splitlines():
        logger.debug(l)

send_initial_dashboard_update(data, config)

run_prologue(data, config, env)
run_command(data, config, env)
run_epilogue(data, config, env)

copy_outputs(data, config, env)
check_outputs(data, config)
check_parrot_cache(data)
