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

(configfile, inputs) = sys.argv[1:3]
args = sys.argv[3:]

for d in os.listdir('.'):
    if d.startswith('CMSSW'):
        break

env = os.environ
env['X509_USER_PROXY'] = os.path.join(d, 'proxy')

edit_process_source(configfile, pickle.loads(base64.b64decode(inputs)))

exit_code = subprocess.call('cmsRun -j report.xml "{0}" {1} > cmssw.log 2>&1'.format(configfile, ' '.join(args)), shell=True, env=env)

sys.exit(exit_code)
