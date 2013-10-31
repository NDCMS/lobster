import os
import shutil
import lobster
import re

def edit_IO_files(cmssw_config_filename, modified_config_filename, config_params):
    shutil.copy(cmssw_config_filename, modified_config_filename)
    (dataset_files, lumis) = config_params
    with open(modified_config_filename, 'a') as modified_config_file:
        fragment = ('import FWCore.ParameterSet.Config as cms'
                    '\nprocess.source.fileNames = cms.untracked.vstring({input_files})'
                    '\nprocess.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))'
                    '\nprocess.source.lumisToProcess = cms.untracked.VLuminosityBlockRange({lumis})')
        modified_config_file.write(fragment.format(input_files=repr(dataset_files), lumis=lumis))

