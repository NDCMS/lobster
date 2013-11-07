#!/usr/bin/env python

import base64
import json
import os
import pickle
import subprocess
import sys

def edit_process_source(cmssw_config_file, config_params):
    (dataset_files, lumis) = config_params
    config = open(cmssw_config_file, 'a')
    with open(cmssw_config_file, 'a') as config:
        fragment = ('import FWCore.ParameterSet.Config as cms'
                    '\nprocess.source.fileNames = cms.untracked.vstring({input_files})'
                    '\nprocess.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))'
                    '\nprocess.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({lumis})')
        config.write(fragment.format(input_files=repr([str(f) for f in dataset_files]), lumis=[str(l) for l in lumis]))

class Shell(object):
    def __init__(self, log, environ=None):
        outlog = log + '.out'
        errlog = log + '.err'
        self._cmdformat = "%%s 1>> '%s' 2>> '%s'; echo $?\n" % (outlog, errlog)
        self._shell = subprocess.Popen("bash", stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, stdin=subprocess.PIPE, env=environ)

    def __lshift__(self, cmd):
        self._shell.stdin.write(self._cmdformat % (cmd,))
        retval = int(self._shell.stdout.readline())

        if retval != 0:
            raise SystemError, retval

(outboxfile, configfile, inputs, args) = sys.argv[1:]

for d in os.listdir('.'):
    if d.startswith('CMSSW'):
        break

env = os.environ
env['X509_USER_PROXY'] = os.path.join(d, 'proxy')

sh = Shell('cmssw', env)
exit_code = 0

with open(task_file, 'r') as f:
    task_list = json.load(f)
    edit_process_source(configfile, pickle.loads(base64.b64.decode(inputs)))

try:
    sh << 'voms-proxy-info'
    sh << 'cd "%s"' % (d,)
    sh << 'eval $(scramv1 runtime -sh)'
    sh << 'cd -'
    sh << 'cmsRun -j report.xml "{0}" {1}'.format(configfile, ' '.join(base64.b64decode(args)))
except:
    exit_code = 123

outbox = tarfile.open(outboxfile, 'w:bz2')
try:
    outbox.add('report.xml')
    outbox.add('cmssw.out')
    outbox.add('cmssw.err')
except:
    pass
outbox.close()

sys.exit(exit_code)
