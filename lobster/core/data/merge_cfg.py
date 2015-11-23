import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
import subprocess
import os
import sys

options = VarParsing('analysis')
options.register('output', mytype=VarParsing.varType.string)
options.register('loginterval', 1000, mytype=VarParsing.varType.int)
options.parseArguments()

process = cms.Process("PickEvent")

process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery = options.loginterval

process.source = cms.Source ("PoolSource",
        fileNames = cms.untracked.vstring(''),
        duplicateCheckMode = cms.untracked.string('noDuplicateCheck')
)

process.out = cms.OutputModule("PoolOutputModule",
        fileName = cms.untracked.string(options.output)
)

process.end = cms.EndPath(process.out)
