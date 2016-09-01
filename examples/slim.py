import FWCore.ParameterSet.Config as cms

process = cms.Process('slim')

process.source = cms.Source("PoolSource", fileNames=cms.untracked.vstring())
process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(10))
process.output = cms.OutputModule("PoolOutputModule",
                                  outputCommands=cms.untracked.vstring(
                                      "drop *", "keep recoTracks_*_*_*"),
                                  fileName=cms.untracked.string('output.root'),
                                  )

process.out = cms.EndPath(process.output)
