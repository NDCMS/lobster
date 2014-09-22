import FWCore.ParameterSet.Config as cms
from FWCore.ParameterSet.VarParsing import VarParsing
import subprocess
import os
import sys

options = VarParsing('analysis')
options.register('chirp', default=None, mytype=VarParsing.varType.string)
options.register('inputs', mult=VarParsing.multiplicity.list, mytype=VarParsing.varType.string)
options.register('output', mytype=VarParsing.varType.string)
options.parseArguments()

if options.chirp:
    for input in options.inputs:
        status = subprocess.call([os.path.join(os.environ.get("PARROT_PATH", "bin"), "chirp_get"),
                                  "-a",
                                  "globus",
                                  options.chirp,
                                  input,
                                  os.path.basename(input)])
        if status != 0:
            sys.exit(500)

process = cms.Process("PickEvent")
process.source = cms.Source ("PoolSource",
	  fileNames = cms.untracked.vstring(''),
      duplicateCheckMode = cms.untracked.string('noDuplicateCheck')
)

process.out = cms.OutputModule("PoolOutputModule",
        fileName = cms.untracked.string(options.output)
)

process.end = cms.EndPath(process.out)
