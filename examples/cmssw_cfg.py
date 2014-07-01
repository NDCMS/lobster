import FWCore.ParameterSet.Config as cms

process = cms.Process('minimaltest')

process.source = cms.Source("PoolSource",
                            fileNames = cms.untracked.vstring('/store/mc/Summer12_DR53X/TTH_Inclusive_M-125_8TeV_pythia6/AODSIM/PU_S10_START53_V7A-v1/00000/04138D6A-0D0A-E211-9223-0025B3E065CA.root')
                            )
process.maxEvents = cms.untracked.PSet(input = cms.untracked.int32(-1))
process.output = cms.OutputModule("PoolOutputModule",
                                  outputCommands = cms.untracked.vstring("drop *", "keep recoTracks_*_*_*"),
                                  fileName = cms.untracked.string('minimal_output.root'),
                                  )

process.out = cms.EndPath(process.output)
