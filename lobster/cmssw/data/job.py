#!/usr/bin/env python

import base64
import json
import os
import pickle
import shutil
import subprocess
import sys

fragment = """import FWCore.ParameterSet.Config as cms
process.source.fileNames = cms.untracked.vstring({input_files})
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
process.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({lumis})"""

def edit_process_source(cmssw_config_file, config_params):
    (dataset_files, lumis) = config_params
    with open(cmssw_config_file, 'a') as config:
        frag = fragment.format(input_files=repr([str(f) for f in dataset_files]), lumis=[str(l) for l in lumis])
        print "--- config file fragment:"
        print frag
        print "---"
        config.write(frag)

(config, inputs) = sys.argv[1:3]
args = sys.argv[3:]

configfile = config.replace(".py", "_mod.py")
shutil.copy2(config, configfile)

for d in os.listdir('.'):
    if d.startswith('CMSSW'):
        break

env = os.environ
env['X509_USER_PROXY'] = os.path.join(d, 'proxy')

edit_process_source(configfile, pickle.loads(base64.b64decode(inputs)))

exit_code = subprocess.call('cmsRun -j report.xml "{0}" {1} > cmssw.log 2>&1'.format(configfile, ' '.join(args)), shell=True, env=env)

sys.exit(exit_code)
