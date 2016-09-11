import FWCore.ParameterSet.Config as cms

process = cms.Process('slim')

process.source = cms.Source("PoolSource", fileNames=cms.untracked.vstring('file:021B993B-4DBB-E511-BBA6-008CFA1111B4.root'))
#process.source = cms.Source("PoolSource", fileNames=cms.untracked.vstring('root://primetest1.crc.nd.edu//store/mc/RunIIFall15MiniAODv2/ttHToNonbb_M125_13TeV_powheg_pythia8/MINIAODSIM/PU25nsData2015v1_76X_mcRun2_asymptotic_v12-v1/00000/021B993B-4DBB-E511-BBA6-008CFA1111B4.root'))
# process.maxEvents = cms.untracked.PSet(input=cms.untracked.int32(10))
process.output = cms.OutputModule("PoolOutputModule",
                                  outputCommands=cms.untracked.vstring(
                                      "keep *"),
                                  fileName=cms.untracked.string('output.root'),
                                  )

process.load('FWCore.MessageService.MessageLogger_cfi')
process.MessageLogger.cerr.FwkReport.reportEvery = 20000

process.out = cms.EndPath(process.output)
