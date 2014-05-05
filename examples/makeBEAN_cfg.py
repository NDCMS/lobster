import sys
import copy
import FWCore.ParameterSet.Config as cms

# === Give values to some basic parameters === #
maxEvents   = -1
reportEvery = 100

# === Python process === #
process = cms.Process( 'BEANs' )


# === Parse external arguments === #
import FWCore.ParameterSet.VarParsing as VarParsing
options = VarParsing.VarParsing("analysis")
# 'jobParams' parameter form:
#
# <era>_<subera>_<type>_<sample number>
#
# <era>                 = 2011, 2012
# <subera> [N/A for MC] = A, B, C...
# <type>                = MC-sigFullSim, MC-sigFastSim, MC-bg, data-PR, data-RR
# <sample number>       = See https://twiki.cern.ch/twiki/bin/view/CMS/TTbarHiggsTauTau#Process_info
#
# Examples:
# 2011_X_MC-sig_2500
# 2011_B_data-PR_-11
# 2012_X_MC-bg_2400
# 2012_B_data-PR_muon
options.register ('jobParams',
                  #'multicrab',
                  #'2012_A_data-PR_-11',	# -1	2012A collisions
                  #'2012_B_data-PR_-11',	# -11	2012B collisions
                  '2012_X_MC-sigFastSim_120',		# 120	signal_M-120
                  #'2012_X_MC-bg_2500',		# 2500  TTbar
                  #'2012_X_MC-bg_2524',		# 2524  TTbar + W
                  #'2012_X_MC-bg_2523',		# 2523  TTbar + Z
                  #'2012_X_MC-bg_2400',		# 2400  W+jets
                  #'2012_X_MC-bg_2800',		# 2800  Z+jets (50<M)
                  #'2012_X_MC-bg_2850',		# 2850  Z+jets (10<M<50)
                  #'2012_X_MC-bg_2700',		# 2700  WW
                  #'2012_X_MC-bg_2701',		# 2701  WZ
                  #'2012_X_MC-bg_2702',		# 2702  ZZ
                  #'2012_X_MC-bg_2504',		# 2504  sT+W
                  #'2012_X_MC-bg_2505',		# 2505  sTbar+W
                  #'2012_X_MC-bg_2600',		# 2600  sT-sCh
                  #'2012_X_MC-bg_2501',		# 2501  sTbar-sCh
                  #'2012_X_MC-bg_2602',		# 2602  sT-tCh
                  #'2012_X_MC-bg_2503',		# 2503  sTbar-tCh
                  #'2012_X_MC-bg_9115',		# 9115  TTH_115_Fast
                  #'2012_X_MC-bg_9120',		# 9120  TTH_120_Fast
                  #'2012_X_MC-bg_9125',		# 9125  TTH_125_Fast
                  VarParsing.VarParsing.multiplicity.singleton,
                  VarParsing.VarParsing.varType.string )
options.maxEvents = maxEvents
#options.outputFile = 'NUT.root'
options.parseArguments()


# === Parse Job Params === #
import shlex;
my_splitter = shlex.shlex(options.jobParams, posix=True);
my_splitter.whitespace = '_';
my_splitter.whitespace_split = True;
jobParams		= list(my_splitter);

# === Job params error checking === #
if len(jobParams) != 4:
	print "ERROR: jobParams set to '" + options.jobParams + "' must have exactly 4 arguments (check config file for details). Terminating."; sys.exit(1);

if (jobParams[0] != "2011") and (jobParams[0] != "2012"):
	print "ERROR: era set to '" + jobParams[0] + "' but it must be '2011' or '2012'"; sys.exit(1);

runOnMC			= ((jobParams[2]).find('MC') != -1);
runOnFastSim	= ((jobParams[2]).find('MC-sigFastSim') != -1);
if (not runOnMC) and ((jobParams[1] != 'A') and (jobParams[1] != 'B') and (jobParams[1] != 'C') and (jobParams[1] != 'D')):
	print "ERROR: job set to run on collision data from sub-era '" + jobParams[1] + "' but it must be 'A', 'B', 'C', or 'D'."; sys.exit(1);

if (jobParams[2] != "data-PR") and (jobParams[2] != "data-RR") and (jobParams[2] != "data-RRr")and (jobParams[2] != "MC-bg") and (jobParams[2] != "MC-sigFullSim") and (jobParams[2] != "MC-sigFastSim"):
	print "ERROR: sample type set to '" + jobParams[2] + "' but it can only be 'data-PR', 'data-RR', 'data-RRr', 'MC-bg', 'MC-sigFullSim', or 'MC-sigFastSim'."; sys.exit(1);

sampleNumber	= int(jobParams[3]);
if (runOnMC and sampleNumber < 0):
	print "ERROR: job set to run on MC but sample number set to '" + sampleNumber + "' when it must be positive."; sys.exit(1);

if (not runOnMC and sampleNumber >= 0):
	print "ERROR: job set to run on collision data but sample number set to '" + sampleNumber + "' when it must be negative."; sys.exit(1);


# === Print some basic info about the job setup === #
print ''
print ' ========================================='
print '     BEAN Production Job'
print ' ========================================='
print ''
print '     Job Type.......%s' % options.jobParams
print '     Max events.....%d' % options.maxEvents
print '     Report every...%d' % reportEvery
print ''
print ' ========================================='
print ''


### Standard and PF work flow

# Standard
runStandardPAT = True
usePFJets      = True
useCaloJets    = False

# PF2PAT
runPF2PAT = True

### Switch on/off selection steps

# Step 0a
useTrigger      = False
# Step 0b
useGoodVertex   = False
# Step 1a
useLooseMuon    = False
# Step 1b
useTightMuon    = False
# Step 2
useMuonVeto     = False
# Step 3
useElectronVeto = False
# Step 4a
use1Jet         = False
# Step 4b
use2Jets        = False
# Step 4c
use3Jets        = False
# Step 5
use4Jets        = False
# Step 6
useBTag         = False

addTriggerMatching = False

# re-run RECO tau production sequence
rerunPFTau = True

### Reference selection

from TopQuarkAnalysis.Configuration.patRefSel_refMuJets import *
# Muons general
muonsUsePV             = True
#muonEmbedTrack         = True
#muonJetsDR             = 0.3
# Standard mouns
muonCut                = 'isGlobalMuon && pt > 10. && abs(eta) < 2.5'
muonCutLoose           = 'pt > 10. && abs(eta) < 2.5'
#looseMuonCut           = ''
#tightMuonCut           = ''
# PF muons
muonCutPF              = 'isGlobalMuon && pt > 10. && abs(eta) < 2.5'
muonCutLoosePF         = 'pt > 10. && abs(eta) < 2.5'
#looseMuonCutPF         = ''
#tightMuonCutPF         = ''
# Standard electrons
electronCut            = 'et > 10. && abs(eta) < 2.5'
# PF electrons
electronCutPF          = 'et > 10. && abs(eta) < 2.5'
electronCutLoosePF     = 'et > 10. && abs(eta) < 2.5'
# Tau cut
tauCut                 = 'pt > 5. && abs(eta) < 2.5 && tauID("decayModeFinding")'
# Calo jets
#jetCut                 = ''
# PF jets
#jetCutPF               = ''
#jetMuonsDRPF           = 0.1

# Trigger and trigger object
#triggerSelectionData       = ''
#triggerObjectSelectionData = ''
#triggerSelectionMC       = ''
#triggerObjectSelectionMC = ''

### Particle flow
### takes effect only, if 'runPF2PAT' = True

postfix = 'PFlow' # needs to be a non-empty string, if 'runStandardPAT' = True

# subtract charged hadronic pile-up particles (from wrong PVs)
# effects also JECs
usePFnoPU       = True # before any top projection
usePfIsoLessCHS = True # switch to new PF isolation with L1Fastjet CHS

# other switches for PF top projections (default: all 'True')
useNoMuon     = True # before electron top projection
useNoElectron = True # before jet top projection
useNoJet      = True # before tau top projection
useNoTau      = False # before MET top projection

# cuts used in top projections
# vertices
#pfVertices  = 'goodOfflinePrimaryVertices'
#pfD0Cut     = 0.2
#pfDzCut     = 0.5
# muons
#pfMuonSelectionCut = 'pt > 5.'
useMuonCutBasePF = False # use minimal (veto) muon selection cut on top of 'pfMuonSelectionCut'
pfMuonIsoConeR03 = False
pfElectronIsoConeR03 = True
#pfMuonCombIsoCut = 0.2
# electrons
#pfElectronSelectionCut  = 'pt > 5. && gsfTrackRef.isNonnull && gsfTrackRef.trackerExpectedHitsInner.numberOfLostHits < 2'
useElectronCutBasePF  = False # use minimal (veto) electron selection cut on top of 'pfElectronSelectionCut'
#pfElectronnIsoConeR03 = False
#pfElectronCombIsoCut  = 0.2

### JEC levels

# levels to be accessible from the jets
# jets are corrected to L3Absolute (MC), L2L3Residual (data) automatically, if enabled here
# and remain uncorrected, if none of these levels is enabled here
useL1FastJet    = True  # needs useL1Offset being off, error otherwise
useL1Offset     = False # needs useL1FastJet being off, error otherwise
useL2Relative   = True
useL3Absolute   = True
useL2L3Residual = True  # takes effect only on data
useL5Flavor     = False
useL7Parton     = False

### Input

# list of input files
useRelVals = False # if 'False', "inputFiles" is used
inputFiles = [] # overwritten, if "useRelVals" is 'True'



if not runOnMC and sampleNumber>=0:
  sys.exit( 'ERROR: Expecting to run on data with sampleNumber>=0.  The sampleNumber must be negative when running on data.' )

if runOnMC and sampleNumber<0:
  sys.exit( 'ERROR: Expecting to run on MC with sampleNumber<0.  The sampleNumber must be positive when running on MC.' )

# maximum number of events
maxInputEvents = 300 # reduce for testing

### Conditions

# GlobalTags (w/o suffix '::All')
#globalTagData = 'GR_R_52_V7'
#globalTagMC   = 'START52_V9'
globalTagData = 'GR_R_53_V10'
globalTagMC   = 'START53_V7G'

### Output

# output file
outputFile = 'ttH_pat2bean_53x.root'

# switch for 'TrigReport'/'TimeReport' at job end
wantSummary = True


###                              End of constants                            ###
###                                                                          ###
### ======================================================================== ###


###
### Basic configuration
###

process.load( "TopQuarkAnalysis.Configuration.patRefSel_basics_cff" )
process.MessageLogger.cerr.FwkReport.reportEvery = reportEvery;
process.options.wantSummary = wantSummary
if runOnMC:
  process.GlobalTag.globaltag = globalTagMC   + '::All'
else:
  process.GlobalTag.globaltag = globalTagData + '::All'


###
### Input configuration
###

process.load( "TopQuarkAnalysis.Configuration.patRefSel_inputModule_cfi" )
if useRelVals:
  from PhysicsTools.PatAlgos.tools.cmsswVersionTools import pickRelValInputFiles
  if runOnMC:
    inputFiles = pickRelValInputFiles( cmsswVersion  = 'CMSSW_5_2_5_cand1'
                                     , relVal        = 'RelValTTbar'
                                     , globalTag     = 'START52_V9'
                                     , maxVersions   = 1
                                     )
  else:
    inputFiles = pickRelValInputFiles( cmsswVersion  = 'CMSSW_5_2_5_cand1'
                                     , relVal        = 'SingleMu'
                                     , dataTier      = 'RECO'
                                     , globalTag     = 'GR_R_52_V7_RelVal_mu2011B'
                                     , maxVersions   = 1
                                     )

#inputFiles = cms.untracked.vstring('/store/data/Run2012A/SingleMu/AOD/PromptReco-v1/000/190/645/FAF2D9E9-7F82-E111-BE0C-003048F1C420.root')
#inputFiles = cms.untracked.vstring('/store/relval/CMSSW_5_2_3_patch3/RelValTTbar/GEN-SIM-RECO/START52_V9_special_120410-v1/0122/0EF8CDEB-1083-E111-846C-002618943937.root')
inputFiles = cms.untracked.vstring(
	#	'/store/mc/Summer12/TTJets_MassiveBinDECAY_TuneZ2star_8TeV-madgraph-tauola/AODSIM/PU_S6_START52_V9-v1/0000/FEFAA4F3-63B8-E111-A65A-00304867924A.root',
	'/store/mc/Summer12_DR53X/TTH_Inclusive_M-125_8TeV_pythia6/AODSIM/PU_S10_START53_V7A-v1/00000/04138D6A-0D0A-E211-9223-0025B3E065CA.root'
)

process.source.fileNames = inputFiles
process.maxEvents.input  = maxInputEvents


###
### Output configuration
###

process.load( "TopQuarkAnalysis.Configuration.patRefSel_outputModule_cff" )
# output file name
process.out.fileName = outputFile
# event content
from PhysicsTools.PatAlgos.patEventContent_cff import patEventContent
process.out.outputCommands += patEventContent
# clear event selection
process.out.SelectEvents.SelectEvents = []


###
### Cleaning and trigger selection configuration
###

### Trigger selection
if runOnMC:
  triggerSelection = triggerSelectionMC
else:
  if useRelVals:
    triggerSelection = triggerSelectionDataRelVals
  else:
    triggerSelection = triggerSelectionData
from TopQuarkAnalysis.Configuration.patRefSel_triggerSelection_cff import triggerResults
process.step0a = triggerResults.clone(
  triggerConditions = [ triggerSelection ]
)

### Good vertex selection
process.load( "TopQuarkAnalysis.Configuration.patRefSel_goodVertex_cfi" )
process.step0b = process.goodOfflinePrimaryVertices.clone( filter = True )

### Event cleaning
process.load( 'TopQuarkAnalysis.Configuration.patRefSel_eventCleaning_cff' )
process.trackingFailureFilter.VertexSource = cms.InputTag( pfVertices )

# For BEAN
process.load('CommonTools/RecoAlgos/HBHENoiseFilterResultProducer_cfi')

process.step0c = process.eventCleaning ## original

#For Fastsim, disable HBHENoiseFilter
if runOnFastSim:
  process.step0c.remove(process.HBHENoiseFilter)
  process.step0c.remove(process.CSCTightHaloFilter)

if runOnMC:
  process.step0c += process.eventCleaningMC
else:
  process.step0c += process.eventCleaningData
  process.step0c += process.HBHENoiseFilterResultProducer


###
### PAT/PF2PAT configuration
###

pfSuffix = 'PF'
if runStandardPAT and runPF2PAT:
  if postfix == '':
    sys.exit( 'ERROR: running standard PAT and PF2PAT in parallel requires a defined "postfix" for PF2PAT' )
  if usePFJets:
    if postfix == 'Add' + pfSuffix or postfix == jetAlgo + pfSuffix:
      sys.exit( 'ERROR: running standard PAT with additional PF jets  and PF2PAT in parallel does not allow for the "postfix" %s'%( postfix ) )
if not runStandardPAT and not runPF2PAT:
  sys.exit( 'ERROR: standard PAT and PF2PAT are both switched off' )

process.load( "PhysicsTools.PatAlgos.patSequences_cff" )
from PhysicsTools.PatAlgos.tools.coreTools import *

# Add PAT trigger information to the configuration
from PhysicsTools.PatAlgos.tools.trigTools import *
switchOnTrigger( process, hltProcess = '*' )


### Check JECs

# JEC set
jecSet      = jecSetBase + 'Calo'
jecSetAddPF = jecSetBase + pfSuffix
jecSetPF    = jecSetAddPF
if usePFnoPU:
  jecSetPF += 'chs'

# JEC levels
if useL1FastJet and useL1Offset:
  sys.exit( 'ERROR: switch off either "L1FastJet" or "L1Offset"' )
jecLevels = []
if useL1FastJet:
  jecLevels.append( 'L1FastJet' )
if useL1Offset:
  jecLevels.append( 'L1Offset' )
if useL2Relative:
  jecLevels.append( 'L2Relative' )
if useL3Absolute:
  jecLevels.append( 'L3Absolute' )
if useL2L3Residual and not runOnMC:
  jecLevels.append( 'L2L3Residual' )
if useL5Flavor:
  jecLevels.append( 'L5Flavor' )
if useL7Parton:
  jecLevels.append( 'L7Parton' )

### Switch configuration

if runPF2PAT:
  if useMuonCutBasePF:
    pfMuonSelectionCut += ' && %s'%( muonCutBase )
  if useElectronCutBasePF:
    pfElectronSelectionCut += ' && %s'%( electronCutBase )
  from PhysicsTools.PatAlgos.tools.pfTools import usePF2PAT
  usePF2PAT( process
           , runPF2PAT      = runPF2PAT
           , runOnMC        = runOnMC
           , jetAlgo        = jetAlgo
           , postfix        = postfix
           , jetCorrections = ( jecSetPF
                              , jecLevels
                              )
           , pvCollection   = cms.InputTag( pfVertices )
           , typeIMetCorrections = True
           )
  applyPostfix( process, 'pfNoPileUp'  , postfix ).enable = usePFnoPU
  applyPostfix( process, 'pfNoMuon'    , postfix ).enable = useNoMuon
  applyPostfix( process, 'pfNoElectron', postfix ).enable = useNoElectron
  applyPostfix( process, 'pfNoJet'     , postfix ).enable = useNoJet
  applyPostfix( process, 'pfNoTau'     , postfix ).enable = useNoTau
  if useL1FastJet:
    applyPostfix( process, 'pfPileUp'   , postfix ).checkClosestZVertex = False
    applyPostfix( process, 'pfPileUpIso', postfix ).checkClosestZVertex = usePfIsoLessCHS
    applyPostfix( process, 'pfJets', postfix ).doAreaFastjet = True
    applyPostfix( process, 'pfJets', postfix ).doRhoFastjet  = False
  applyPostfix( process, 'pfMuonsFromVertex'    , postfix ).d0Cut    = pfD0Cut
  applyPostfix( process, 'pfMuonsFromVertex'    , postfix ).dzCut    = pfDzCut
  applyPostfix( process, 'pfSelectedMuons'      , postfix ).cut = pfMuonSelectionCut
  applyPostfix( process, 'pfIsolatedMuons'      , postfix ).isolationCut = pfMuonCombIsoCut
  if pfMuonIsoConeR03:
    applyPostfix( process, 'pfIsolatedMuons', postfix ).isolationValueMapsCharged  = cms.VInputTag( cms.InputTag( 'muPFIsoValueCharged03' + postfix )
                                                                                                  )
    applyPostfix( process, 'pfIsolatedMuons', postfix ).deltaBetaIsolationValueMap = cms.InputTag( 'muPFIsoValuePU03' + postfix )
    applyPostfix( process, 'pfIsolatedMuons', postfix ).isolationValueMapsNeutral  = cms.VInputTag( cms.InputTag( 'muPFIsoValueNeutral03' + postfix )
                                                                                                  , cms.InputTag( 'muPFIsoValueGamma03' + postfix )
                                                                                                  )
    applyPostfix( process, 'patMuons', postfix ).isolationValues.pfNeutralHadrons   = cms.InputTag( 'muPFIsoValueNeutral03' + postfix )
    applyPostfix( process, 'patMuons', postfix ).isolationValues.pfPUChargedHadrons = cms.InputTag( 'muPFIsoValuePU03' + postfix )
    applyPostfix( process, 'patMuons', postfix ).isolationValues.pfPhotons          = cms.InputTag( 'muPFIsoValueGamma03' + postfix )
    applyPostfix( process, 'patMuons', postfix ).isolationValues.pfChargedHadrons   = cms.InputTag( 'muPFIsoValueCharged03' + postfix )
  applyPostfix( process, 'pfElectronsFromVertex'    , postfix ).d0Cut    = pfD0Cut
  applyPostfix( process, 'pfElectronsFromVertex'    , postfix ).dzCut    = pfDzCut
  applyPostfix( process, 'pfSelectedElectrons'      , postfix ).cut = pfElectronSelectionCut
  applyPostfix( process, 'pfIsolatedElectrons'      , postfix ).isolationCut = pfElectronCombIsoCut
  if pfElectronIsoConeR03:
    applyPostfix( process, 'pfIsolatedElectrons', postfix ).isolationValueMapsCharged  = cms.VInputTag( cms.InputTag( 'elPFIsoValueCharged03PFId' + postfix )
                                                                                                       )
    applyPostfix( process, 'pfIsolatedElectrons', postfix ).deltaBetaIsolationValueMap = cms.InputTag( 'elPFIsoValuePU03PFId' + postfix )
    applyPostfix( process, 'pfIsolatedElectrons', postfix ).isolationValueMapsNeutral  = cms.VInputTag( cms.InputTag( 'elPFIsoValueNeutral03PFId' + postfix )
                                                                                                      , cms.InputTag( 'elPFIsoValueGamma03PFId'   + postfix )
                                                                                                      )
    applyPostfix( process, 'patElectrons', postfix ).isolationValues.pfNeutralHadrons   = cms.InputTag( 'elPFIsoValueNeutral03PFId' + postfix )
    applyPostfix( process, 'patElectrons', postfix ).isolationValues.pfPUChargedHadrons = cms.InputTag( 'elPFIsoValuePU03PFId' + postfix )
    applyPostfix( process, 'patElectrons', postfix ).isolationValues.pfPhotons          = cms.InputTag( 'elPFIsoValueGamma03PFId' + postfix )
    applyPostfix( process, 'patElectrons', postfix ).isolationValues.pfChargedHadrons   = cms.InputTag( 'elPFIsoValueCharged03PFId' + postfix )

from TopQuarkAnalysis.Configuration.patRefSel_refMuJets_cfi import *

# remove MC matching, object cleaning, objects etc.
jecLevelsCalo = copy.copy( jecLevels )
if runStandardPAT:
  if not runOnMC:
    runOnData( process )
  # subsequent jet area calculations needed for L1FastJet on RECO jets
  if useCaloJets and useL1FastJet:
    if useRelVals:
      process.ak5CaloJets = ak5CaloJets.clone( doAreaFastjet = True )
      process.ak5JetID    = ak5JetID.clone()
      process.ak5CaloJetSequence = cms.Sequence(
        process.ak5CaloJets
      * process.ak5JetID
      )
      from PhysicsTools.PatAlgos.tools.jetTools import switchJetCollection
      switchJetCollection( process
                         , cms.InputTag( jetAlgo.lower() + 'CaloJets' )
                         , doJTA            = True
                         , doBTagging       = True
                         , jetCorrLabel     = ( jecSet, jecLevels )
                         , doType1MET       = False
                         , genJetCollection = cms.InputTag( jetAlgo.lower() + 'GenJets' )
                         , doJetID          = True
                         )
    else:
      print 'WARNING patRefSel_muJets_test_cfg.py:'
      print '        L1FastJet JECs are not available for AK5Calo jets in this data due to missing jet area computation;'
      print '        switching to   L1Offset   !!!'
      jecLevelsCalo.insert( 0, 'L1Offset' )
      jecLevelsCalo.remove( 'L1FastJet' )
      process.patJetCorrFactors.levels = jecLevelsCalo
      #process.patJetCorrFactors.useRho = False # FIXME: does not apply
  if usePFJets:
    if useL1FastJet:
      process.ak5PFJets = ak5PFJets.clone( doAreaFastjet = True )
    from PhysicsTools.PatAlgos.tools.jetTools import addJetCollection
    from PhysicsTools.PatAlgos.tools.metTools import addPfMET
    addJetCollection( process
                    , cms.InputTag( jetAlgo.lower() + pfSuffix + 'Jets' )
                    , jetAlgo
                    , pfSuffix
                    , doJTA            = True
                    , doBTagging       = True
                    , jetCorrLabel     = ( jecSetAddPF, jecLevels )
                    , doType1MET       = False
                    , doL1Cleaning     = False
                    , doL1Counters     = True
                    , genJetCollection = cms.InputTag( jetAlgo.lower() + 'GenJets' )
                    , doJetID          = True
                    )
    addPfMET( process
            , jetAlgo + pfSuffix
            )
removeSpecificPATObjects( process
                          , names = [ 'Photons' ]
                          ) # includes 'removeCleaning'
if runPF2PAT:
  if not runOnMC:
    runOnData( process
             , names = [ 'PFAll' ]
             , postfix = postfix
             )
    removeSpecificPATObjects( process
                              , names = [ 'Taus' ]
                              , postfix = postfix
                              ) # includes 'removeCleaning'

# JetCorrFactorsProducer configuration has to be fixed in standard work flow after a call to 'runOnData()':
if runStandardPAT:
  process.patJetCorrFactors.payload = jecSet
  process.patJetCorrFactors.levels  = jecLevelsCalo

# additional event content has to be (re-)added _after_ the call to 'removeCleaning()':
process.out.outputCommands += [ 'keep edmTriggerResults_*_*_*'
                              , 'keep *_hltTriggerSummaryAOD_*_*'
                              , 'keep L1GlobalTriggerReadoutRecord_gtDigis_*_*'
                              # vertices and beam spot
                              , 'keep *_offlineBeamSpot_*_*'
                              , 'keep *_offlinePrimaryVertices*_*_*'
                              , 'keep *_goodOfflinePrimaryVertices*_*_*'
                              , 'keep patTriggerObjects_patTrigger_*_*'
                              , 'keep patTriggerFilters_patTrigger_*_*'
                              , 'keep patTriggerPaths_patTrigger_*_*'
                              , 'keep patTriggerEvent_patTriggerEvent_*_*'
                              , 'keep *_l1extraParticles_*_*'
                              , 'keep *_kt6PFJetsCentralChargedPileUp_rho_*'
                              , 'keep *_kt6PFJetsCentralNeutral_rho_*'
                              , 'keep *_kt6PFJetsCentralNeutralTight_rho_*'
                              , 'keep double_kt6PFJetsCentral_rho_RECO'
                              , 'keep double_kt6PFJets*_*_*'
                              , 'keep *_patConversions*_*_*'
                              , 'keep *_selectedPatPhotons__*'
                              #, "keep *_puJetId_*_*" # input variables
                              #, "keep *_puJetMva_*_*"
                                ]


keepSC = True
keepTK = True

if keepSC:
  process.out.outputCommands += [ 'keep *_correctedHybridSuperClusters_*_*'
                                , 'keep *_correctedMulti5x5SuperClustersWithPreshower_*_*'
                                ]
if keepTK:
  process.highPtTracks = cms.EDFilter (
    "TrackSelector",
    src = cms.InputTag ('generalTracks'),
    cut = cms.string ('pt > 15')
  )
#  process.out.outputCommands += [ 'keep *_generalTracks_*_*'
  process.out.outputCommands += [ 'keep *_highPtTracks_*_*'
                                ]
if runOnMC:
  process.out.outputCommands += [ 'keep GenEventInfoProduct_*_*_*'
                                , 'keep recoGenParticles_*_*_*'
                                , 'keep *_addPileupInfo_*_*'
                                , 'keep LHEEventProduct_*_*_*'
                                ]


###
### Additional configuration
###

#goodPatJetsAddPFLabel = 'goodPatJets' + jetAlgo + pfSuffix

if runStandardPAT:

  ### Muons

  #process.intermediatePatMuons = intermediatePatMuons.clone()
  #process.loosePatMuons        = loosePatMuons.clone()
  #process.step1a               = step1a.clone()
  #process.tightPatMuons        = tightPatMuons.clone()
  #process.step1b               = step1b.clone()
  process.step2                = step2.clone( src = cms.InputTag( 'selectedPatMuons' ) )

  #if usePFJets:
    #loosePatMuonsAddPF = loosePatMuons.clone()
    #loosePatMuonsAddPF.checkOverlaps.jets.src = cms.InputTag( goodPatJetsAddPFLabel )
    #setattr( process, 'loosePatMuons' + jetAlgo + pfSuffix, loosePatMuonsAddPF )
    #step1aAddPF = step1a.clone( src = cms.InputTag( 'loosePatMuons' + jetAlgo + pfSuffix ) )
    #setattr( process, 'step1a' + jetAlgo + pfSuffix, step1aAddPF )
    #tightPatMuonsAddPF = tightPatMuons.clone( src = cms.InputTag( 'loosePatMuons' + jetAlgo + pfSuffix ) )
    #setattr( process, 'tightPatMuons' + jetAlgo + pfSuffix, tightPatMuonsAddPF )
    #step1bAddPF = step1b.clone( src = cms.InputTag( 'tightPatMuons' + jetAlgo + pfSuffix ) )
    #setattr( process, 'step1b' + jetAlgo + pfSuffix, step1bAddPF )

  ### Jets

  #process.kt6PFJets = kt6PFJets.clone( src          = cms.InputTag( 'particleFlow' )
  #                                   , doRhoFastjet = True
  #                                   )
  # compute FastJet rho to correct isolation
  #process.kt6PFJetsForIsolation = kt6PFJets.clone()
  #process.kt6PFJetsForIsolation.Rho_EtaMax = cms.double(2.5)

  #process.patDefaultSequence.replace( process.patJetCorrFactors
  #                                  , process.kt6PFJets * process.kt6PFJetsForIsolation * process.patJetCorrFactors
  #                                  )
  #process.patDefaultSequence.replace( process.patJetCorrFactors
  #                                   , process.kt6PFJets * process.patJetCorrFactors
  #                                   )
  #process.out.outputCommands.append( 'keep double_kt6PFJets_*_' + process.name_() )
  process.out.outputCommands.append( 'keep double_*_*_' + process.name_() )
  if useL1FastJet:
    process.patJetCorrFactors.useRho = True
    if usePFJets:
      getattr( process, 'patJetCorrFactors' + jetAlgo + pfSuffix ).useRho = True

  #process.goodPatJets = goodPatJets.clone()
  process.step4a      = step4a.clone()
  process.step4b      = step4b.clone()
  #process.step4c      = step4c.clone()
  process.step5       = step5.clone()

  #if usePFJets:
  #  goodPatJetsAddPF = goodPatJets.clone( src = cms.InputTag( 'selectedPatJets' + jetAlgo + pfSuffix ) )
  #  setattr( process, goodPatJetsAddPFLabel, goodPatJetsAddPF )
  #  step4aAddPF = step4a.clone( src = cms.InputTag( goodPatJetsAddPFLabel ) )
  #  setattr( process, 'step4a' + jetAlgo + pfSuffix, step4aAddPF )
  #  step4bAddPF = step4b.clone( src = cms.InputTag( goodPatJetsAddPFLabel ) )
  #  setattr( process, 'step4b' + jetAlgo + pfSuffix, step4bAddPF )
  #  step4cAddPF = step4c.clone( src = cms.InputTag( goodPatJetsAddPFLabel ) )
  #  setattr( process, 'step4c' + jetAlgo + pfSuffix, step4cAddPF )
  #  step5AddPF = step5.clone( src = cms.InputTag( goodPatJetsAddPFLabel ) )
  #  setattr( process, 'step5' + jetAlgo + pfSuffix, step5AddPF )

  ### Electrons

  process.step3 = step3.clone( src = cms.InputTag( 'selectedPatElectrons' ) )


  ### Taus

  process.selectedPatTaus.cut = tauCut

if runPF2PAT:

  ### Muons

  #intermediatePatMuonsPF = intermediatePatMuons.clone( src = cms.InputTag( 'selectedPatMuons' + postfix ) )
  #setattr( process, 'intermediatePatMuons' + postfix, intermediatePatMuonsPF )

  #loosePatMuonsPF = loosePatMuons.clone( src = cms.InputTag( 'intermediatePatMuons' + postfix ) )
  #setattr( process, 'loosePatMuons' + postfix, loosePatMuonsPF )
  #getattr( process, 'loosePatMuons' + postfix ).checkOverlaps.jets.src = cms.InputTag( 'goodPatJets' + postfix )

  #step1aPF = step1a.clone( src = cms.InputTag( 'loosePatMuons' + postfix ) )
  #setattr( process, 'step1a' + postfix, step1aPF )

  #tightPatMuonsPF = tightPatMuons.clone( src = cms.InputTag( 'loosePatMuons' + postfix ) )
  #setattr( process, 'tightPatMuons' + postfix, tightPatMuonsPF )

  #step1bPF = step1b.clone( src = cms.InputTag( 'tightPatMuons' + postfix ) )
  #setattr( process, 'step1b' + postfix, step1bPF )

  #step2PF = step2.clone( src = cms.InputTag( 'selectedPatMuons' + postfix ) )
  #setattr( process, 'step2' + postfix, step2PF )

  ### Jets

 # kt6PFJetsPF = kt6PFJets.clone( doRhoFastjet = True )
 # setattr( process, 'kt6PFJets' + postfix, kt6PFJetsPF )
 # getattr( process, 'patPF2PATSequence' + postfix).replace( getattr( process, 'pfNoElectron' + postfix )
 #                                                         , getattr( process, 'pfNoElectron' + postfix ) * getattr( process, 'kt6PFJets' + postfix )
 #                                                         )
  #if useL1FastJet:
  #  applyPostfix( process, 'patJetCorrFactors', postfix ).rho = cms.InputTag( 'kt6PFJets' + postfix, 'rho' )
  #process.out.outputCommands.append( 'keep double_kt6PFJets' + postfix + '_*_' + process.name_() )

  #goodPatJetsPF = goodPatJets.clone( src = cms.InputTag( 'selectedPatJets' + postfix ) )
  #setattr( process, 'goodPatJets' + postfix, goodPatJetsPF )
  #getattr( process, 'goodPatJets' + postfix ).checkOverlaps.muons.src = cms.InputTag( 'intermediatePatMuons' + postfix )

  #step4aPF = step4a.clone( src = cms.InputTag( 'goodPatJets' + postfix ) )
  #setattr( process, 'step4a' + postfix, step4aPF )
  #step4bPF = step4b.clone( src = cms.InputTag( 'goodPatJets' + postfix ) )
  #setattr( process, 'step4b' + postfix, step4bPF )
  #step4cPF = step4c.clone( src = cms.InputTag( 'goodPatJets' + postfix ) )
  #setattr( process, 'step4c' + postfix, step4cPF )
  #step5PF = step5.clone( src = cms.InputTag( 'goodPatJets' + postfix ) )
  #setattr( process, 'step5'  + postfix, step5PF  )

  ### Electrons

  step3PF = step3.clone( src = cms.InputTag( 'selectedPatElectrons' + postfix ) )
  setattr( process, 'step3' + postfix, step3PF )

#process.out.outputCommands.append( 'keep *_intermediatePatMuons*_*_*' )
#process.out.outputCommands.append( 'keep *_loosePatMuons*_*_*' )
#process.out.outputCommands.append( 'keep *_tightPatMuons*_*_*' )
#process.out.outputCommands.append( 'keep *_goodPatJets*_*_*' )


###
### Selection configuration
###

if runStandardPAT:

  ### Muons

  process.patMuons.usePV      = muonsUsePV
  process.patMuons.embedTrack = muonEmbedTrack

  process.selectedPatMuons.cut = muonCut
  selectedPatMuonsLoose = applyPostfix(process, 'selectedPatMuons', '').clone()
  selectedPatMuonsLoose.src = cms.InputTag( 'patMuons' )
  selectedPatMuonsLoose.cut = muonCutLoose                                     
  setattr(process,'selectedPatMuonsLoose',selectedPatMuonsLoose)
  
  #process.intermediatePatMuons.preselection = looseMuonCut

  #process.loosePatMuons.checkOverlaps.jets.deltaR = muonJetsDR
  if usePFJets:
    print "not using loose muons"
    #getattr( process, 'loosePatMuons' + jetAlgo + pfSuffix ).checkOverlaps.jets.deltaR = muonJetsDR

  #process.tightPatMuons.preselection = tightMuonCut
  if usePFJets:
    print "not using tight muons"
    #getattr( process, 'tightPatMuons' + jetAlgo + pfSuffix ).preselection = tightMuonCut

  ### Jets

  #process.goodPatJets.preselection = jetCut
  #if usePFJets:
  #  getattr( process, goodPatJetsAddPFLabel ).preselection               = jetCutPF
  #  getattr( process, goodPatJetsAddPFLabel ).checkOverlaps.muons.deltaR = jetMuonsDRPF

  ### Electrons

  process.patElectrons.electronIDSources = electronIDSources

  process.selectedPatElectrons.cut = electronCut

if runPF2PAT:

  applyPostfix( process, 'patMuons', postfix ).usePV      = muonsUsePV
  applyPostfix( process, 'patMuons', postfix ).embedTrack = muonEmbedTrack

  applyPostfix( process, 'selectedPatMuons', postfix ).cut = muonCutPF

  #getattr( process, 'intermediatePatMuons' + postfix ).preselection = looseMuonCutPF

  #getattr( process, 'loosePatMuons' + postfix ).preselection              = looseMuonCutPF
  #getattr( process, 'loosePatMuons' + postfix ).checkOverlaps.jets.deltaR = muonJetsDR

  #getattr( process, 'tightPatMuons' + postfix ).preselection = tightMuonCutPF

  ### Jets

  #getattr( process, 'goodPatJets' + postfix ).preselection               = jetCutPF
  #getattr( process, 'goodPatJets' + postfix ).checkOverlaps.muons.deltaR = jetMuonsDRPF

  ### Electrons

  applyPostfix( process, 'patElectrons', postfix ).electronIDSources = electronIDSources

  applyPostfix( process, 'selectedPatElectrons', postfix ).cut = electronCutPF


###
### Trigger matching
###

if addTriggerMatching:

  if runOnMC:
    triggerObjectSelection = triggerObjectSelectionMC
  else:
    if useRelVals:
      triggerObjectSelection = triggerObjectSelectionDataRelVals
    else:
      triggerObjectSelection = triggerObjectSelectionData
  ### Trigger matching configuration
  from PhysicsTools.PatAlgos.triggerLayer1.triggerProducer_cfi import patTrigger
  from TopQuarkAnalysis.Configuration.patRefSel_triggerMatching_cfi import patMuonTriggerMatch
  from PhysicsTools.PatAlgos.tools.trigTools import *
  if runStandardPAT:
    triggerProducer = patTrigger.clone()
    setattr( process, 'patTrigger', triggerProducer )
    process.triggerMatch = patMuonTriggerMatch.clone( matchedCuts = triggerObjectSelection )
    switchOnTriggerMatchEmbedding( process
                                 , triggerMatchers = [ 'triggerMatch' ]
                                 )
    removeCleaningFromTriggerMatching( process )
    #process.intermediatePatMuons.src = cms.InputTag( 'selectedPatMuonsTriggerMatch' )
  if runPF2PAT:
    triggerProducerPF = patTrigger.clone()
    setattr( process, 'patTrigger' + postfix, triggerProducerPF )
    triggerMatchPF = patMuonTriggerMatch.clone( matchedCuts = triggerObjectSelection )
    setattr( process, 'triggerMatch' + postfix, triggerMatchPF )
    switchOnTriggerMatchEmbedding( process
                                 , triggerProducer = 'patTrigger' + postfix
                                 , triggerMatchers = [ 'triggerMatch' + postfix ]
                                 , sequence        = 'patPF2PATSequence' + postfix
                                 , postfix         = postfix
                                 )
    removeCleaningFromTriggerMatching( process
                                     , sequence = 'patPF2PATSequence' + postfix
                                     )
    #getattr( process, 'intermediatePatMuons' + postfix ).src = cms.InputTag( 'selectedPatMuons' + postfix + 'TriggerMatch' )


if runPF2PAT:
  ##
  ## LOOSE LEPTON ISOLATION
  ##

  muPFIsoDepositChargedLoose = applyPostfix(process, 'muPFIsoDepositCharged', postfix).clone()
  muPFIsoDepositChargedAllLoose = applyPostfix(process, 'muPFIsoDepositChargedAll', postfix).clone()
  muPFIsoDepositNeutralLoose = applyPostfix(process, 'muPFIsoDepositNeutral', postfix).clone()
  muPFIsoDepositGammaLoose = applyPostfix(process, 'muPFIsoDepositGamma', postfix).clone()
  muPFIsoDepositPULoose = applyPostfix(process, 'muPFIsoDepositPU', postfix).clone()

  muPFIsoDepositChargedLoose.src = cms.InputTag( 'pfSelectedMuonsLoose' + postfix )
  muPFIsoDepositChargedAllLoose.src = cms.InputTag( 'pfSelectedMuonsLoose' + postfix )
  muPFIsoDepositNeutralLoose.src = cms.InputTag( 'pfSelectedMuonsLoose' + postfix )
  muPFIsoDepositGammaLoose.src = cms.InputTag( 'pfSelectedMuonsLoose' + postfix )
  muPFIsoDepositPULoose.src = cms.InputTag( 'pfSelectedMuonsLoose' + postfix )

  setattr(process,'muPFIsoDepositChargedLoose'+postfix,muPFIsoDepositChargedLoose)
  setattr(process,'muPFIsoDepositChargedAllLoose'+postfix,muPFIsoDepositChargedAllLoose)
  setattr(process,'muPFIsoDepositNeutralLoose'+postfix,muPFIsoDepositNeutralLoose)
  setattr(process,'muPFIsoDepositGammaLoose'+postfix,muPFIsoDepositGammaLoose)
  setattr(process,'muPFIsoDepositPULoose'+postfix,muPFIsoDepositPULoose)

  getattr( process, 'muonPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'muPFIsoDepositCharged'+postfix),
                                                                          getattr(process, 'muPFIsoDepositCharged'+postfix)+getattr(process, 'muPFIsoDepositChargedLoose'+postfix)
                                                                          )
  getattr( process, 'muonPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'muPFIsoDepositChargedAll'+postfix),
                                                                          getattr(process, 'muPFIsoDepositChargedAll'+postfix)+getattr(process, 'muPFIsoDepositChargedAllLoose'+postfix)
                                                                          )
  getattr( process, 'muonPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'muPFIsoDepositNeutral'+postfix),
                                                                          getattr(process, 'muPFIsoDepositNeutral'+postfix)+getattr(process, 'muPFIsoDepositNeutralLoose'+postfix)
                                                                          )
  getattr( process, 'muonPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'muPFIsoDepositGamma'+postfix),
                                                                          getattr(process, 'muPFIsoDepositGamma'+postfix)+getattr(process, 'muPFIsoDepositGammaLoose'+postfix)
                                                                          )
  getattr( process, 'muonPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'muPFIsoDepositPU'+postfix),
                                                                          getattr(process, 'muPFIsoDepositPU'+postfix)+getattr(process, 'muPFIsoDepositPULoose'+postfix)
                                                                          )

  #Muon,Charged
  isoValMuonWithChargedDR03 = applyPostfix(process, 'muPFIsoValueCharged03', postfix).clone()
  isoValMuonWithChargedDR03.deposits[0].deltaR = 0.3
  isoValMuonWithChargedDR03Loose = applyPostfix(process, 'muPFIsoValueCharged03', postfix).clone()
  isoValMuonWithChargedDR03Loose.deposits[0].deltaR = 0.3
  isoValMuonWithChargedDR03Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositChargedLoose' + postfix )
  setattr(process,'isoValMuonWithChargedDR03'+postfix,isoValMuonWithChargedDR03)
  setattr(process,'isoValMuonWithChargedDR03Loose'+postfix,isoValMuonWithChargedDR03Loose)
  # Install alternative isolation in path
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValueCharged03'+postfix),
                                                            getattr(process, 'muPFIsoValueCharged03'+postfix)+getattr(process, 'isoValMuonWithChargedDR03'+postfix)+getattr(process, 'isoValMuonWithChargedDR03Loose'+postfix)
                                                            )
  #Muon,Charged All
  isoValMuonWithChargedAllDR03 = applyPostfix(process, 'muPFIsoValueChargedAll03', postfix).clone()
  isoValMuonWithChargedAllDR03.deposits[0].deltaR = 0.3
  isoValMuonWithChargedAllDR03Loose = applyPostfix(process, 'muPFIsoValueChargedAll03', postfix).clone()
  isoValMuonWithChargedAllDR03Loose.deposits[0].deltaR = 0.3
  isoValMuonWithChargedAllDR03Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositChargedAllLoose' + postfix )
  setattr(process,'isoValMuonWithChargedAllDR03'+postfix,isoValMuonWithChargedAllDR03)
  setattr(process,'isoValMuonWithChargedAllDR03Loose'+postfix,isoValMuonWithChargedAllDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValueChargedAll03'+postfix),
                                                            getattr(process, 'muPFIsoValueChargedAll03'+postfix)+getattr(process, 'isoValMuonWithChargedAllDR03'+postfix)+getattr(process, 'isoValMuonWithChargedAllDR03Loose'+postfix)
                                                            )
  #Muon, Neutral
  isoValMuonWithNeutralDR03 = applyPostfix(process, 'muPFIsoValueNeutral03', postfix).clone()
  isoValMuonWithNeutralDR03.deposits[0].deltaR = 0.3
  isoValMuonWithNeutralDR03Loose = applyPostfix(process, 'muPFIsoValueNeutral03', postfix).clone()
  isoValMuonWithNeutralDR03Loose.deposits[0].deltaR = 0.3
  isoValMuonWithNeutralDR03Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositNeutralLoose' + postfix )
  setattr(process,'isoValMuonWithNeutralDR03'+postfix,isoValMuonWithNeutralDR03)
  setattr(process,'isoValMuonWithNeutralDR03Loose'+postfix,isoValMuonWithNeutralDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValueNeutral03'+postfix),
                                                            getattr(process, 'muPFIsoValueNeutral03'+postfix)+getattr(process, 'isoValMuonWithNeutralDR03'+postfix)+getattr(process, 'isoValMuonWithNeutralDR03Loose'+postfix)
                                                            )
  #Muon, Photons
  isoValMuonWithPhotonsDR03 = applyPostfix(process, 'muPFIsoValueGamma03', postfix).clone()
  isoValMuonWithPhotonsDR03.deposits[0].deltaR = 0.3
  isoValMuonWithPhotonsDR03Loose = applyPostfix(process, 'muPFIsoValueGamma03', postfix).clone()
  isoValMuonWithPhotonsDR03Loose.deposits[0].deltaR = 0.3
  isoValMuonWithPhotonsDR03Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositGammaLoose' + postfix )
  setattr(process,'isoValMuonWithPhotonsDR03'+postfix,isoValMuonWithPhotonsDR03)
  setattr(process,'isoValMuonWithPhotonsDR03Loose'+postfix,isoValMuonWithPhotonsDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValueGamma03'+postfix),
                                                            getattr(process, 'muPFIsoValueGamma03'+postfix)+getattr(process, 'isoValMuonWithPhotonsDR03'+postfix)+getattr(process, 'isoValMuonWithPhotonsDR03Loose'+postfix)
                                                            )
  #Muon, PU
  isoValMuonWithPUDR03 = applyPostfix(process, 'muPFIsoValuePU03', postfix).clone()
  isoValMuonWithPUDR03.deposits[0].deltaR = 0.3
  isoValMuonWithPUDR03Loose = applyPostfix(process, 'muPFIsoValuePU03', postfix).clone()
  isoValMuonWithPUDR03Loose.deposits[0].deltaR = 0.3
  isoValMuonWithPUDR03Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositPULoose' + postfix )
  setattr(process,'isoValMuonWithPUDR03'+postfix,isoValMuonWithPUDR03)
  setattr(process,'isoValMuonWithPUDR03Loose'+postfix,isoValMuonWithPUDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValuePU03'+postfix),
                                                            getattr(process, 'muPFIsoValuePU03'+postfix)+getattr(process, 'isoValMuonWithPUDR03'+postfix)+getattr(process, 'isoValMuonWithPUDR03Loose'+postfix)
                                                            )

  #Muon,Charged
  isoValMuonWithChargedDR04 = applyPostfix(process, 'muPFIsoValueCharged04', postfix).clone()
  isoValMuonWithChargedDR04.deposits[0].deltaR = 0.4
  isoValMuonWithChargedDR04Loose = applyPostfix(process, 'muPFIsoValueCharged04', postfix).clone()
  isoValMuonWithChargedDR04Loose.deposits[0].deltaR = 0.4
  isoValMuonWithChargedDR04Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositChargedLoose' + postfix )
  setattr(process,'isoValMuonWithChargedDR04'+postfix,isoValMuonWithChargedDR04)
  setattr(process,'isoValMuonWithChargedDR04Loose'+postfix,isoValMuonWithChargedDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValueCharged04'+postfix),
                                                            getattr(process, 'muPFIsoValueCharged04'+postfix)+getattr(process, 'isoValMuonWithChargedDR04'+postfix)+getattr(process, 'isoValMuonWithChargedDR04Loose'+postfix)
                                                            )
  #Muon,Charged All
  isoValMuonWithChargedAllDR04 = applyPostfix(process, 'muPFIsoValueChargedAll04', postfix).clone()
  isoValMuonWithChargedAllDR04.deposits[0].deltaR = 0.4
  isoValMuonWithChargedAllDR04Loose = applyPostfix(process, 'muPFIsoValueChargedAll04', postfix).clone()
  isoValMuonWithChargedAllDR04Loose.deposits[0].deltaR = 0.4
  isoValMuonWithChargedAllDR04Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositChargedAllLoose' + postfix )
  setattr(process,'isoValMuonWithChargedAllDR04'+postfix,isoValMuonWithChargedAllDR04)
  setattr(process,'isoValMuonWithChargedAllDR04Loose'+postfix,isoValMuonWithChargedAllDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValueChargedAll04'+postfix),
                                                            getattr(process, 'muPFIsoValueChargedAll04'+postfix)+getattr(process, 'isoValMuonWithChargedAllDR04'+postfix)+getattr(process, 'isoValMuonWithChargedAllDR04Loose'+postfix)
                                                            )
  #Muon, Neutral
  isoValMuonWithNeutralDR04 = applyPostfix(process, 'muPFIsoValueNeutral04', postfix).clone()
  isoValMuonWithNeutralDR04.deposits[0].deltaR = 0.4
  isoValMuonWithNeutralDR04Loose = applyPostfix(process, 'muPFIsoValueNeutral04', postfix).clone()
  isoValMuonWithNeutralDR04Loose.deposits[0].deltaR = 0.4
  isoValMuonWithNeutralDR04Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositNeutralLoose' + postfix )
  setattr(process,'isoValMuonWithNeutralDR04'+postfix,isoValMuonWithNeutralDR04)
  setattr(process,'isoValMuonWithNeutralDR04Loose'+postfix,isoValMuonWithNeutralDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValueNeutral04'+postfix),
                                                            getattr(process, 'muPFIsoValueNeutral04'+postfix)+getattr(process, 'isoValMuonWithNeutralDR04'+postfix)+getattr(process, 'isoValMuonWithNeutralDR04Loose'+postfix)
                                                            )
  #Muon, Photons
  isoValMuonWithPhotonsDR04 = applyPostfix(process, 'muPFIsoValueGamma04', postfix).clone()
  isoValMuonWithPhotonsDR04.deposits[0].deltaR = 0.4
  isoValMuonWithPhotonsDR04Loose = applyPostfix(process, 'muPFIsoValueGamma04', postfix).clone()
  isoValMuonWithPhotonsDR04Loose.deposits[0].deltaR = 0.4
  isoValMuonWithPhotonsDR04Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositGammaLoose' + postfix )
  setattr(process,'isoValMuonWithPhotonsDR04'+postfix,isoValMuonWithPhotonsDR04)
  setattr(process,'isoValMuonWithPhotonsDR04Loose'+postfix,isoValMuonWithPhotonsDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValueGamma04'+postfix),
                                                            getattr(process, 'muPFIsoValueGamma04'+postfix)+getattr(process, 'isoValMuonWithPhotonsDR04'+postfix)+getattr(process, 'isoValMuonWithPhotonsDR04Loose'+postfix)
                                                            )
  #Muon, PU
  isoValMuonWithPUDR04 = applyPostfix(process, 'muPFIsoValuePU04', postfix).clone()
  isoValMuonWithPUDR04.deposits[0].deltaR = 0.4
  isoValMuonWithPUDR04Loose = applyPostfix(process, 'muPFIsoValuePU04', postfix).clone()
  isoValMuonWithPUDR04Loose.deposits[0].deltaR = 0.4
  isoValMuonWithPUDR04Loose.deposits[0].src = cms.InputTag ( 'muPFIsoDepositPULoose' + postfix )
  setattr(process,'isoValMuonWithPUDR04'+postfix,isoValMuonWithPUDR04)
  setattr(process,'isoValMuonWithPUDR04Loose'+postfix,isoValMuonWithPUDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'muPFIsoValuePU04'+postfix),
                                                            getattr(process, 'muPFIsoValuePU04'+postfix)+getattr(process, 'isoValMuonWithPUDR04'+postfix)+getattr(process, 'isoValMuonWithPUDR04Loose'+postfix)
                                                            )
  applyPostfix(process,'patMuons',postfix).isolationValues.user = cms.VInputTag("isoValMuonWithChargedDR03"+postfix
                                                                                , "isoValMuonWithNeutralDR03"+postfix
                                                                                , "isoValMuonWithPhotonsDR03"+postfix
                                                                                , "isoValMuonWithPUDR03"+postfix
                                                                                , "isoValMuonWithChargedDR04"+postfix
                                                                                , "isoValMuonWithNeutralDR04"+postfix
                                                                                , "isoValMuonWithPhotonsDR04"+postfix
                                                                                , "isoValMuonWithPUDR04"+postfix
    )

  elPFIsoDepositChargedLoose = applyPostfix(process, 'elPFIsoDepositCharged', postfix).clone()
  elPFIsoDepositChargedAllLoose = applyPostfix(process, 'elPFIsoDepositChargedAll', postfix).clone()
  elPFIsoDepositNeutralLoose = applyPostfix(process, 'elPFIsoDepositNeutral', postfix).clone()
  elPFIsoDepositGammaLoose = applyPostfix(process, 'elPFIsoDepositGamma', postfix).clone()
  elPFIsoDepositPULoose = applyPostfix(process, 'elPFIsoDepositPU', postfix).clone()

  elPFIsoDepositChargedLoose.src = cms.InputTag( 'pfSelectedElectronsLoose' + postfix )
  elPFIsoDepositChargedAllLoose.src = cms.InputTag( 'pfSelectedElectronsLoose' + postfix )
  elPFIsoDepositNeutralLoose.src = cms.InputTag( 'pfSelectedElectronsLoose' + postfix )
  elPFIsoDepositGammaLoose.src = cms.InputTag( 'pfSelectedElectronsLoose' + postfix )
  elPFIsoDepositPULoose.src = cms.InputTag( 'pfSelectedElectronsLoose' + postfix )

  setattr(process,'elPFIsoDepositChargedLoose'+postfix,elPFIsoDepositChargedLoose)
  setattr(process,'elPFIsoDepositChargedAllLoose'+postfix,elPFIsoDepositChargedAllLoose)
  setattr(process,'elPFIsoDepositNeutralLoose'+postfix,elPFIsoDepositNeutralLoose)
  setattr(process,'elPFIsoDepositGammaLoose'+postfix,elPFIsoDepositGammaLoose)
  setattr(process,'elPFIsoDepositPULoose'+postfix,elPFIsoDepositPULoose)

  getattr( process, 'electronPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'elPFIsoDepositCharged'+postfix),
                                                                              getattr(process, 'elPFIsoDepositCharged'+postfix)+getattr(process, 'elPFIsoDepositChargedLoose'+postfix)
                                                                              )
  getattr( process, 'electronPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'elPFIsoDepositChargedAll'+postfix),
                                                                              getattr(process, 'elPFIsoDepositChargedAll'+postfix)+getattr(process, 'elPFIsoDepositChargedAllLoose'+postfix)
                                                                              )
  getattr( process, 'electronPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'elPFIsoDepositNeutral'+postfix),
                                                                              getattr(process, 'elPFIsoDepositNeutral'+postfix)+getattr(process, 'elPFIsoDepositNeutralLoose'+postfix)
                                                                              )
  getattr( process, 'electronPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'elPFIsoDepositGamma'+postfix),
                                                                              getattr(process, 'elPFIsoDepositGamma'+postfix)+getattr(process, 'elPFIsoDepositGammaLoose'+postfix)
                                                                              )
  getattr( process, 'electronPFIsolationDepositsSequence' + postfix ).replace(getattr(process, 'elPFIsoDepositPU'+postfix),
                                                                              getattr(process, 'elPFIsoDepositPU'+postfix)+getattr(process, 'elPFIsoDepositPULoose'+postfix)
                                                                              )

  #Electron, Charged
  isoValElectronWithChargedDR03 = applyPostfix(process, 'elPFIsoValueCharged03PFId', postfix).clone()
  isoValElectronWithChargedDR03.deposits[0].deltaR = 0.3
  isoValElectronWithChargedDR03Loose = applyPostfix(process, 'elPFIsoValueCharged03PFId', postfix).clone()
  isoValElectronWithChargedDR03Loose.deposits[0].deltaR = 0.3
  isoValElectronWithChargedDR03Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositChargedLoose' + postfix )
  setattr(process,'isoValElectronWithChargedDR03'+postfix,isoValElectronWithChargedDR03)
  setattr(process,'isoValElectronWithChargedDR03Loose'+postfix,isoValElectronWithChargedDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValueCharged03PFId'+postfix),
                                                            getattr(process, 'elPFIsoValueCharged03PFId'+postfix)+getattr(process, 'isoValElectronWithChargedDR03'+postfix)+getattr(process, 'isoValElectronWithChargedDR03Loose'+postfix)
                                                            )
  #Electron, Charged All
  isoValElectronWithChargedAllDR03 = applyPostfix(process, 'elPFIsoValueChargedAll03PFId', postfix).clone()
  isoValElectronWithChargedAllDR03.deposits[0].deltaR = 0.3
  isoValElectronWithChargedAllDR03Loose = applyPostfix(process, 'elPFIsoValueChargedAll03PFId', postfix).clone()
  isoValElectronWithChargedAllDR03Loose.deposits[0].deltaR = 0.3
  isoValElectronWithChargedAllDR03Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositChargedAllLoose' + postfix )
  setattr(process,'isoValElectronWithChargedAllDR03'+postfix,isoValElectronWithChargedAllDR03)
  setattr(process,'isoValElectronWithChargedAllDR03Loose'+postfix,isoValElectronWithChargedAllDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValueChargedAll03PFId'+postfix),
                                                            getattr(process, 'elPFIsoValueChargedAll03PFId'+postfix)+getattr(process, 'isoValElectronWithChargedAllDR03'+postfix)+getattr(process, 'isoValElectronWithChargedAllDR03Loose'+postfix)
                                                            )
  #Electron, Neutral
  isoValElectronWithNeutralDR03 = applyPostfix(process, 'elPFIsoValueNeutral03PFId', postfix).clone()
  isoValElectronWithNeutralDR03.deposits[0].deltaR = 0.3
  isoValElectronWithNeutralDR03Loose = applyPostfix(process, 'elPFIsoValueNeutral03PFId', postfix).clone()
  isoValElectronWithNeutralDR03Loose.deposits[0].deltaR = 0.3
  isoValElectronWithNeutralDR03Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositNeutralLoose' + postfix )
  setattr(process,'isoValElectronWithNeutralDR03'+postfix,isoValElectronWithNeutralDR03)
  setattr(process,'isoValElectronWithNeutralDR03Loose'+postfix,isoValElectronWithNeutralDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValueNeutral03PFId'+postfix),
                                                            getattr(process, 'elPFIsoValueNeutral03PFId'+postfix)+getattr(process, 'isoValElectronWithNeutralDR03'+postfix)+getattr(process, 'isoValElectronWithNeutralDR03Loose'+postfix)
                                                            )
  #Electron, Photons
  isoValElectronWithPhotonsDR03 = applyPostfix(process, 'elPFIsoValueGamma03PFId', postfix).clone()
  isoValElectronWithPhotonsDR03.deposits[0].deltaR = 0.3
  isoValElectronWithPhotonsDR03Loose = applyPostfix(process, 'elPFIsoValueGamma03PFId', postfix).clone()
  isoValElectronWithPhotonsDR03Loose.deposits[0].deltaR = 0.3
  isoValElectronWithPhotonsDR03Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositGammaLoose' + postfix )
  setattr(process,'isoValElectronWithPhotonsDR03'+postfix,isoValElectronWithPhotonsDR03)
  setattr(process,'isoValElectronWithPhotonsDR03Loose'+postfix,isoValElectronWithPhotonsDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValueGamma03PFId'+postfix),
                                                            getattr(process, 'elPFIsoValueGamma03PFId'+postfix)+getattr(process, 'isoValElectronWithPhotonsDR03'+postfix)+getattr(process, 'isoValElectronWithPhotonsDR03Loose'+postfix)
                                                            )
  #Electron, PU
  isoValElectronWithPUDR03 = applyPostfix(process, 'elPFIsoValuePU03PFId', postfix).clone()
  isoValElectronWithPUDR03.deposits[0].deltaR = 0.3
  isoValElectronWithPUDR03Loose = applyPostfix(process, 'elPFIsoValuePU03PFId', postfix).clone()
  isoValElectronWithPUDR03Loose.deposits[0].deltaR = 0.3
  isoValElectronWithPUDR03Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositPULoose' + postfix )
  setattr(process,'isoValElectronWithPUDR03'+postfix,isoValElectronWithPUDR03)
  setattr(process,'isoValElectronWithPUDR03Loose'+postfix,isoValElectronWithPUDR03Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValuePU03PFId'+postfix),
                                                            getattr(process, 'elPFIsoValuePU03PFId'+postfix)+getattr(process, 'isoValElectronWithPUDR03'+postfix)+getattr(process, 'isoValElectronWithPUDR03Loose'+postfix)
                                                            )

  #Electron, Charged
  isoValElectronWithChargedDR04 = applyPostfix(process, 'elPFIsoValueCharged04PFId', postfix).clone()
  isoValElectronWithChargedDR04.deposits[0].deltaR = 0.4
  isoValElectronWithChargedDR04Loose = applyPostfix(process, 'elPFIsoValueCharged04PFId', postfix).clone()
  isoValElectronWithChargedDR04Loose.deposits[0].deltaR = 0.4
  isoValElectronWithChargedDR04Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositChargedLoose' + postfix )
  setattr(process,'isoValElectronWithChargedDR04'+postfix,isoValElectronWithChargedDR04)
  setattr(process,'isoValElectronWithChargedDR04Loose'+postfix,isoValElectronWithChargedDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValueCharged04PFId'+postfix),
                                                            getattr(process, 'elPFIsoValueCharged04PFId'+postfix)+getattr(process, 'isoValElectronWithChargedDR04'+postfix)+getattr(process, 'isoValElectronWithChargedDR04Loose'+postfix)
                                                            )
  #Electron, Charged All
  isoValElectronWithChargedAllDR04 = applyPostfix(process, 'elPFIsoValueChargedAll04PFId', postfix).clone()
  isoValElectronWithChargedAllDR04.deposits[0].deltaR = 0.4
  isoValElectronWithChargedAllDR04Loose = applyPostfix(process, 'elPFIsoValueChargedAll04PFId', postfix).clone()
  isoValElectronWithChargedAllDR04Loose.deposits[0].deltaR = 0.4
  isoValElectronWithChargedAllDR04Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositChargedAllLoose' + postfix )
  setattr(process,'isoValElectronWithChargedAllDR04'+postfix,isoValElectronWithChargedAllDR04)
  setattr(process,'isoValElectronWithChargedAllDR04Loose'+postfix,isoValElectronWithChargedAllDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValueChargedAll04PFId'+postfix),
                                                            getattr(process, 'elPFIsoValueChargedAll04PFId'+postfix)+getattr(process, 'isoValElectronWithChargedAllDR04'+postfix)+getattr(process, 'isoValElectronWithChargedAllDR04Loose'+postfix)
                                                            )
  #Electron, Neutral
  isoValElectronWithNeutralDR04 = applyPostfix(process, 'elPFIsoValueNeutral04PFId', postfix).clone()
  isoValElectronWithNeutralDR04.deposits[0].deltaR = 0.4
  isoValElectronWithNeutralDR04Loose = applyPostfix(process, 'elPFIsoValueNeutral04PFId', postfix).clone()
  isoValElectronWithNeutralDR04Loose.deposits[0].deltaR = 0.4
  isoValElectronWithNeutralDR04Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositNeutralLoose' + postfix )
  setattr(process,'isoValElectronWithNeutralDR04'+postfix,isoValElectronWithNeutralDR04)
  setattr(process,'isoValElectronWithNeutralDR04Loose'+postfix,isoValElectronWithNeutralDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValueNeutral04PFId'+postfix),
                                                            getattr(process, 'elPFIsoValueNeutral04PFId'+postfix)+getattr(process, 'isoValElectronWithNeutralDR04'+postfix)+getattr(process, 'isoValElectronWithNeutralDR04Loose'+postfix)
                                                            )
  #Electron, Photons
  isoValElectronWithPhotonsDR04 = applyPostfix(process, 'elPFIsoValueGamma04PFId', postfix).clone()
  isoValElectronWithPhotonsDR04.deposits[0].deltaR = 0.4
  isoValElectronWithPhotonsDR04Loose = applyPostfix(process, 'elPFIsoValueGamma04PFId', postfix).clone()
  isoValElectronWithPhotonsDR04Loose.deposits[0].deltaR = 0.4
  isoValElectronWithPhotonsDR04Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositGammaLoose' + postfix )
  setattr(process,'isoValElectronWithPhotonsDR04'+postfix,isoValElectronWithPhotonsDR04)
  setattr(process,'isoValElectronWithPhotonsDR04Loose'+postfix,isoValElectronWithPhotonsDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValueGamma04PFId'+postfix),
                                                            getattr(process, 'elPFIsoValueGamma04PFId'+postfix)+getattr(process, 'isoValElectronWithPhotonsDR04'+postfix)+getattr(process, 'isoValElectronWithPhotonsDR04Loose'+postfix)
                                                            )
  #Electron, PU
  isoValElectronWithPUDR04 = applyPostfix(process, 'elPFIsoValuePU04PFId', postfix).clone()
  isoValElectronWithPUDR04.deposits[0].deltaR = 0.4
  isoValElectronWithPUDR04Loose = applyPostfix(process, 'elPFIsoValuePU04PFId', postfix).clone()
  isoValElectronWithPUDR04Loose.deposits[0].deltaR = 0.4
  isoValElectronWithPUDR04Loose.deposits[0].src = cms.InputTag ( 'elPFIsoDepositPULoose' + postfix )
  setattr(process,'isoValElectronWithPUDR04'+postfix,isoValElectronWithPUDR04)
  setattr(process,'isoValElectronWithPUDR04Loose'+postfix,isoValElectronWithPUDR04Loose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'elPFIsoValuePU04PFId'+postfix),
                                                            getattr(process, 'elPFIsoValuePU04PFId'+postfix)+getattr(process, 'isoValElectronWithPUDR04'+postfix)+getattr(process, 'isoValElectronWithPUDR04Loose'+postfix)
                                                            )

  applyPostfix(process,'patElectrons',postfix).isolationValues.user = cms.VInputTag("isoValElectronWithChargedDR03"+postfix
                                                                                    , "isoValElectronWithNeutralDR03"+postfix
                                                                                    , "isoValElectronWithPhotonsDR03"+postfix
                                                                                    , "isoValElectronWithPUDR03"+postfix
                                                                                    , "isoValElectronWithChargedDR04"+postfix
                                                                                    , "isoValElectronWithNeutralDR04"+postfix
                                                                                    , "isoValElectronWithPhotonsDR04"+postfix
                                                                                    , "isoValElectronWithPUDR04"+postfix
    )


  from PhysicsTools.PatAlgos.tools.pfTools import adaptPFMuons
  from PhysicsTools.PatAlgos.tools.pfTools import adaptPFElectrons

  ## Muons
  pfMuonsFromVertexLoose = applyPostfix(process, 'pfMuonsFromVertex', postfix).clone()
  pfMuonsFromVertexLoose.d0Cut = cms.double(-1.0)
  pfMuonsFromVertexLoose.dzCut = cms.double(-1.0)
  pfMuonsFromVertexLoose.d0SigCut = cms.double(-1.0)
  pfMuonsFromVertexLoose.dzSigCut = cms.double(-1.0)
  setattr(process,'pfMuonsFromVertexLoose'+postfix,pfMuonsFromVertexLoose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'pfMuonsFromVertex'+postfix),
                                                            getattr(process, 'pfMuonsFromVertex'+postfix)+getattr(process, 'pfMuonsFromVertexLoose'+postfix)
                                                            )

  pfSelectedMuonsLoose = applyPostfix(process, 'pfSelectedMuons', postfix).clone()
  pfSelectedMuonsLoose.src = cms.InputTag( 'pfMuonsFromVertexLoose' + postfix )
  setattr(process,'pfSelectedMuonsLoose'+postfix,pfSelectedMuonsLoose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'pfSelectedMuons'+postfix),
                                                            getattr(process, 'pfSelectedMuons'+postfix)+getattr(process, 'pfSelectedMuonsLoose'+postfix)
                                                            )

  pfIsolatedMuonsLoose = applyPostfix(process, 'pfIsolatedMuons', postfix).clone()
  pfIsolatedMuonsLoose.src = cms.InputTag( 'pfSelectedMuonsLoose' + postfix )
  pfIsolatedMuonsLoose.isolationCut = cms.double(999.0)
  pfIsolatedMuonsLoose.combinedIsolationCut = cms.double(999.0)
  pfIsolatedMuonsLoose.isolationValueMapsCharged = cms.VInputTag(cms.InputTag('isoValMuonWithChargedDR04Loose'+postfix))
  pfIsolatedMuonsLoose.deltaBetaIsolationValueMap = cms.InputTag('isoValMuonWithPUDR04Loose'+postfix)
  pfIsolatedMuonsLoose.isolationValueMapsNeutral = cms.VInputTag(cms.InputTag('isoValMuonWithNeutralDR04Loose'+postfix), cms.InputTag('isoValMuonWithPhotonsDR04Loose'+postfix))
  setattr(process,'pfIsolatedMuonsLoose'+postfix,pfIsolatedMuonsLoose)

  patMuonsLoose = applyPostfix(process, 'patMuons', postfix).clone()
  patMuonsLoose.addResolutions = cms.bool(False)
  patMuonsLoose.pfMuonSource = cms.InputTag( 'pfIsolatedMuonsLoose' + postfix )
  patMuonsLoose.genParticleMatch = cms.InputTag( 'muonMatchLoose' + postfix )

  setattr(process,'patMuonsLoose'+postfix,patMuonsLoose)

  adaptPFMuons( process, getattr(process, 'patMuonsLoose'+postfix), postfix )

  getattr(process, 'patMuonsLoose'+postfix ).isoDeposits = cms.PSet(pfNeutralHadrons = cms.InputTag("muPFIsoDepositNeutralLoose"+postfix),
                                                                    pfChargedAll = cms.InputTag("muPFIsoDepositChargedAllLoose"+postfix),
                                                                    pfPUChargedHadrons = cms.InputTag("muPFIsoDepositPULoose"+postfix),
                                                                    pfPhotons = cms.InputTag("muPFIsoDepositGammaLoose"+postfix),
                                                                    pfChargedHadrons = cms.InputTag("muPFIsoDepositChargedLoose"+postfix),
    )
  getattr(process, 'patMuonsLoose'+postfix ).isolationValues = cms.PSet(pfNeutralHadrons = cms.InputTag("isoValMuonWithNeutralDR04Loose"+postfix),
                                                                        pfChargedAll = cms.InputTag("isoValMuonWithChargedAllDR04Loose"+postfix),
                                                                        pfPUChargedHadrons = cms.InputTag("isoValMuonWithPUDR04Loose"+postfix),
                                                                        pfPhotons = cms.InputTag("isoValMuonWithPhotonsDR04Loose"+postfix),
                                                                        pfChargedHadrons = cms.InputTag("isoValMuonWithChargedDR04Loose"+postfix),
    )
  getattr(process, 'patMuonsLoose'+postfix ).isolationValues.user = cms.VInputTag("isoValMuonWithChargedDR03Loose"+postfix
                                                                                  , "isoValMuonWithNeutralDR03Loose"+postfix
                                                                                  , "isoValMuonWithPhotonsDR03Loose"+postfix
                                                                                  , "isoValMuonWithPUDR03Loose"+postfix
                                                                                  , "isoValMuonWithChargedDR04Loose"+postfix
                                                                                  , "isoValMuonWithNeutralDR04Loose"+postfix
                                                                                  , "isoValMuonWithPhotonsDR04Loose"+postfix
                                                                                  , "isoValMuonWithPUDR04Loose"+postfix
    )
  getattr(process, 'patMuonsLoose'+postfix ).pfMuonSource = cms.InputTag( 'pfIsolatedMuonsLoose' + postfix )

  muonMatchLoose = applyPostfix(process, 'muonMatch', postfix).clone()
  muonMatchLoose.src = cms.InputTag( 'pfIsolatedMuonsLoose' + postfix )
  setattr(process,'muonMatchLoose'+postfix,muonMatchLoose)

  applyPostfix( process, 'muonMatch', postfix ).src = cms.InputTag( 'pfIsolatedMuons' + postfix )

  selectedPatMuonsLoose = applyPostfix(process, 'selectedPatMuons', postfix).clone()
  selectedPatMuonsLoose.src = cms.InputTag( 'patMuonsLoose' + postfix )
  selectedPatMuonsLoose.cut = muonCutLoosePF
  setattr(process,'selectedPatMuonsLoose'+postfix,selectedPatMuonsLoose)

  ## Electrons
  pfElectronsFromVertexLoose = applyPostfix(process, 'pfElectronsFromVertex', postfix).clone()
  pfElectronsFromVertexLoose.d0Cut = cms.double(-1.0)
  pfElectronsFromVertexLoose.dzCut = cms.double(-1.0)
  pfElectronsFromVertexLoose.d0SigCut = cms.double(-1.0)
  pfElectronsFromVertexLoose.dzSigCut = cms.double(-1.0)
  setattr(process,'pfElectronsFromVertexLoose'+postfix,pfElectronsFromVertexLoose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'pfElectronsFromVertex'+postfix),
                                                            getattr(process, 'pfElectronsFromVertex'+postfix)+getattr(process, 'pfElectronsFromVertexLoose'+postfix)
                                                            )

  pfSelectedElectronsLoose = applyPostfix(process, 'pfSelectedElectrons', postfix).clone()
  pfSelectedElectronsLoose.src = cms.InputTag( 'pfElectronsFromVertexLoose' + postfix )
  setattr(process,'pfSelectedElectronsLoose'+postfix,pfSelectedElectronsLoose)
  getattr( process, 'patPF2PATSequence' + postfix ).replace(getattr(process, 'pfSelectedElectrons'+postfix),
                                                            getattr(process, 'pfSelectedElectrons'+postfix)+getattr(process, 'pfSelectedElectronsLoose'+postfix)
                                                            )

  pfIsolatedElectronsLoose = applyPostfix(process, 'pfIsolatedElectrons', postfix).clone()
  pfIsolatedElectronsLoose.src = cms.InputTag( 'pfSelectedElectronsLoose' + postfix )
  pfIsolatedElectronsLoose.isolationCut = cms.double(999.0)
  pfIsolatedElectronsLoose.combinedIsolationCut = cms.double(999.0)
  pfIsolatedElectronsLoose.isolationValueMapsCharged = cms.VInputTag(cms.InputTag('isoValElectronWithChargedDR03Loose'+postfix))
  pfIsolatedElectronsLoose.deltaBetaIsolationValueMap = cms.InputTag('isoValElectronWithPUDR03Loose'+postfix)
  pfIsolatedElectronsLoose.isolationValueMapsNeutral = cms.VInputTag(cms.InputTag('isoValElectronWithNeutralDR03Loose'+postfix), cms.InputTag('isoValElectronWithPhotonsDR03Loose'+postfix))
  setattr(process,'pfIsolatedElectronsLoose'+postfix,pfIsolatedElectronsLoose)

  patElectronsLoose = applyPostfix(process, 'patElectrons', postfix).clone()
  patElectronsLoose.pfElectronSource = cms.InputTag( 'pfIsolatedElectronsLoose' + postfix )

  setattr(process,'patElectronsLoose'+postfix,patElectronsLoose)

  adaptPFElectrons( process, getattr(process, 'patElectronsLoose'+postfix), postfix )

  getattr(process, 'patElectronsLoose'+postfix ).isoDeposits = cms.PSet(pfNeutralHadrons = cms.InputTag("elPFIsoDepositNeutralLoose"+postfix),
                                                                        pfChargedAll = cms.InputTag("elPFIsoDepositChargedAllLoose"+postfix),
                                                                        pfPUChargedHadrons = cms.InputTag("elPFIsoDepositPULoose"+postfix),
                                                                        pfPhotons = cms.InputTag("elPFIsoDepositGammaLoose"+postfix),
                                                                        pfChargedHadrons = cms.InputTag("elPFIsoDepositChargedLoose"+postfix),
    )
  getattr(process, 'patElectronsLoose'+postfix ).isolationValues = cms.PSet(pfNeutralHadrons = cms.InputTag("isoValElectronWithNeutralDR03Loose"+postfix),
                                                                            pfChargedAll = cms.InputTag("isoValElectronWithChargedAllDR03Loose"+postfix),
                                                                            pfPUChargedHadrons = cms.InputTag("isoValElectronWithPUDR03Loose"+postfix),
                                                                            pfPhotons = cms.InputTag("isoValElectronWithPhotonsDR03Loose"+postfix),
                                                                            pfChargedHadrons = cms.InputTag("isoValElectronWithChargedDR03Loose"+postfix),
    )
  getattr(process, 'patElectronsLoose'+postfix ).isolationValues.user = cms.VInputTag("isoValElectronWithChargedDR03Loose"+postfix
                                                                                      , "isoValElectronWithNeutralDR03Loose"+postfix
                                                                                      , "isoValElectronWithPhotonsDR03Loose"+postfix
                                                                                      , "isoValElectronWithPUDR03Loose"+postfix
                                                                                      , "isoValElectronWithChargedDR04Loose"+postfix
                                                                                      , "isoValElectronWithNeutralDR04Loose"+postfix
                                                                                      , "isoValElectronWithPhotonsDR04Loose"+postfix
                                                                                      , "isoValElectronWithPUDR04Loose"+postfix
    )
  getattr(process, 'patElectronsLoose'+postfix ).pfElectronSource = cms.InputTag( 'pfIsolatedElectronsLoose' + postfix )

  selectedPatElectronsLoose = applyPostfix(process, 'selectedPatElectrons', postfix).clone()
  selectedPatElectronsLoose.src = cms.InputTag( 'patElectronsLoose' + postfix )
  selectedPatElectronsLoose.cut = electronCutLoosePF
  setattr(process,'selectedPatElectronsLoose'+postfix,selectedPatElectronsLoose)


  if not runOnMC:
    process.looseLeptonSequence = cms.Sequence(
      getattr(process, 'pfIsolatedMuonsLoose'+postfix) +
      getattr(process, 'patMuonsLoose'+postfix) +
      getattr(process, 'selectedPatMuonsLoose'+postfix) +
      getattr(process, 'selectedPatMuonsLoose') +
      getattr(process, 'pfIsolatedElectronsLoose'+postfix) +
      getattr(process, 'patElectronsLoose'+postfix) +
      getattr(process, 'selectedPatElectronsLoose'+postfix)
      )

  if runOnMC:
    process.looseLeptonSequence = cms.Sequence(
      getattr(process, 'pfIsolatedMuonsLoose'+postfix) +
      getattr(process, 'muonMatchLoose'+postfix) +
      getattr(process, 'patMuonsLoose'+postfix) +
      getattr(process, 'selectedPatMuonsLoose'+postfix) +
      getattr(process, 'selectedPatMuonsLoose') +
      getattr(process, 'pfIsolatedElectronsLoose'+postfix) +
      getattr(process, 'patElectronsLoose'+postfix) +
      getattr(process, 'selectedPatElectronsLoose'+postfix)
      )


### Darren Specific Stuff for BEAN

## Needed for calculating track isolation quantities
from RecoMuon.MuonIsolationProducers.caloExtractorByAssociatorBlocks_cff import *
from RecoMuon.MuonIsolationProducers.trackExtractorBlocks_cff import *
process.load('Configuration.StandardSequences.MagneticField_38T_cff')
process.load('Configuration.StandardSequences.Reconstruction_cff')

process.BNproducer = cms.EDProducer('BEANmaker',
                                    calometTag = cms.InputTag("none"),
                                    pfmetTag = cms.InputTag("patMETsPFlow"),
                                    pfmetTag_type1correctedRECO = cms.InputTag("pfType1CorrectedMet"),
                                    pfmetTag_uncorrectedPF2PAT  = cms.InputTag("patPFMetPFlow"),
                                    pfmetTag_uncorrectedRECO    = cms.InputTag("pfMETPFlow"),
                                    tcmetTag = cms.InputTag("none"),
                                    eleTag = cms.InputTag("selectedPatElectrons"),
                                    pfeleTag = cms.InputTag("selectedPatElectronsPFlow"),
                                    pfeleLooseTag = cms.InputTag("selectedPatElectronsLoosePFlow"),
                                    genParticleTag = cms.InputTag("genParticles"),
                                    calojetTag = cms.InputTag("none"),
                                    pfjetTag = cms.InputTag("selectedPatJetsPFlow"),
                                    genjetTag = cms.InputTag("ak5GenJets"),
                                    muonTag = cms.InputTag("selectedPatMuons"),
				    muonLooseTag = cms.InputTag("selectedPatMuonsLoose"),
                                    pfmuonTag = cms.InputTag("selectedPatMuonsPFlow"),
                                    pfmuonLooseTag = cms.InputTag("selectedPatMuonsLoosePFlow"),
                                    cocktailmuonTag = cms.InputTag("none"),
                                    photonTag = cms.InputTag("none"),
                                    EBsuperclusterTag = cms.InputTag("correctedHybridSuperClusters"),
                                    EEsuperclusterTag = cms.InputTag("correctedMulti5x5SuperClustersWithPreshower"),
                                    reducedBarrelRecHitCollection = cms.InputTag("reducedEcalRecHitsEB"),
                                    reducedEndcapRecHitCollection = cms.InputTag("reducedEcalRecHitsEE"),
                                    trackTag = cms.InputTag("generalTracks"),
                                    tauTag = cms.InputTag("selectedPatTaus"),
                                    triggerResultsTag = cms.InputTag("TriggerResults::HLT"),
                                    gtSource = cms.InputTag("gtDigis"),
                                    pvTag = cms.InputTag("offlinePrimaryVertices"),
                                    triggerSummaryTag = cms.InputTag("hltTriggerSummaryAOD"),
                                    dcsTag = cms.InputTag("scalersRawToDigi"),
                                    hltProcessName = cms.string("HLT"),
                                    eventWeight = cms.double(0.0126976903794279),
                                    minSCEt = cms.double(10),
                                    minPhotonEt = cms.double(10),
                                    minJetPt = cms.double(10),
                                    minTrackPt = cms.double(10),
                                    verbose = cms.bool(True),
                                    fillTrackHitInfo = cms.bool(False),
                                    fillTrackIsoInfo = cms.bool(False),
                                    CaloExtractorPSet  = cms.PSet(MIsoCaloExtractorByAssociatorTowersBlock),
                                    TrackExtractorPSet = cms.PSet(MIsoTrackExtractorBlock),
                                    sample = cms.int32(sampleNumber),
                                    maxAbsZ = cms.untracked.double(24)
                                    )


process.q2weights = cms.EDProducer('Q2Weights')



# For BEAN
process.load('RecoMuon/MuonIdentification/refitMuons_cfi')

# For BEAN
process.skimMuon = cms.EDFilter("SkimMuon",
                                ptCutMu1 = cms.untracked.double(15),
                                ptCutMu2 = cms.untracked.double(0),
                                caloIsoCut = cms.untracked.double(999999999),
                                trackIsoCut = cms.untracked.double(999999999),
                                minNumGoodMu = cms.untracked.int32(1)
                                )




###
### Scheduling
###

# MVA electron ID

process.load( "EGamma.EGammaAnalysisTools.electronIdMVAProducer_cfi" )
process.eidMVASequence = cms.Sequence(
  process.mvaTrigV0
+ process.mvaNonTrigV0
)

### PU Jet-ID
# load the PU JetID sequence
process.load("CMGTools.External.pujetidsequence_cff")

# OK, out of laziness, I'm only going to configure the pu jet ID for our PF2PAT jets
process.puJetIdChs.vertexes = 'goodOfflinePrimaryVertices'
process.puJetIdChs.jets = 'selectedPatJetsPFlow'

process.puJetMvaChs.vertexes = 'goodOfflinePrimaryVertices'
process.puJetMvaChs.jets = 'selectedPatJetsPFlow'


# The additional sequence

if runStandardPAT:
  process.patAddOnSequence = cms.Sequence(
    #process.intermediatePatMuons
  #* process.goodPatJets
  #* process.loosePatMuons
  #* process.tightPatMuons
  )
if runPF2PAT:
  patAddOnSequence = cms.Sequence(
    #getattr( process, 'intermediatePatMuons' + postfix )
  #* getattr( process, 'goodPatJets' + postfix )
  #* getattr( process, 'loosePatMuons' + postfix )
  #* getattr( process, 'tightPatMuons' + postfix )
  )
  setattr( process, 'patAddOnSequence' + postfix, patAddOnSequence )

if runPF2PAT:
  process.patConversions = cms.EDProducer("PATConversionProducer",
      electronSource = cms.InputTag("selectedPatElectronsPFlow")
      )


# The paths
if runStandardPAT:

  if useCaloJets:

    process.p = cms.Path()
    if useTrigger:
      process.p += process.step0a
    process.p += process.goodOfflinePrimaryVertices
    if useGoodVertex:
      process.p += process.step0b
    process.p += process.step0c
    process.p += process.eidMVASequence
    if rerunPFTau:
      process.load("RecoTauTag.Configuration.RecoPFTauTag_cff")
      process.p += process.PFTau
    if useL1FastJet and useRelVals:
      process.p += process.ak5CaloJetSequence
    process.p += process.patDefaultSequence
    if usePFJets:
      process.p.remove( getattr( process, 'patJetCorrFactors'                    + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'jetTracksAssociatorAtVertex'          + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'impactParameterTagInfos'              + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'secondaryVertexTagInfos'              + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'softMuonTagInfos'                     + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'secondaryVertexNegativeTagInfos'      + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'jetBProbabilityBJetTags'              + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'jetProbabilityBJetTags'               + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'trackCountingHighPurBJetTags'         + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'trackCountingHighEffBJetTags'         + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'simpleSecondaryVertexHighEffBJetTags' + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'simpleSecondaryVertexHighPurBJetTags' + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'combinedSecondaryVertexBJetTags'      + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'combinedSecondaryVertexMVABJetTags'   + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'softMuonBJetTags'                     + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'softMuonByPtBJetTags'                 + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'softMuonByIP3dBJetTags'               + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'simpleSecondaryVertexNegativeHighEffBJetTags' + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'simpleSecondaryVertexNegativeHighPurBJetTags' + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'negativeTrackCountingHighEffJetTags'          + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'negativeTrackCountingHighPurJetTags'          + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'patJetCharge'                         + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'patJetPartonMatch'                    + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'patJetGenJetMatch'                    + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'patJetPartonAssociation'              + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'patJetFlavourAssociation'             + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'patJets'                              + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'patMETs'                              + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'selectedPatJets'                      + jetAlgo + pfSuffix ) )
      process.p.remove( getattr( process, 'countPatJets'                         + jetAlgo + pfSuffix ) )
    process.p += process.patAddOnSequence
#    if useLooseMuon:
#      process.p += process.step1a
#    if useTightMuon:
#      process.p += process.step1b
    if useMuonVeto:
      process.p += process.step2
    if useElectronVeto:
      process.p += process.step3
    if use1Jet:
      process.p += process.step4a
    if use2Jets:
      process.p += process.step4b
#    if use3Jets:
#      process.p += process.step4c
    if use4Jets:
      process.p += process.step5
    process.out.SelectEvents.SelectEvents.append( 'p' )

  if usePFJets:

    pAddPF = cms.Path()
    if useTrigger:
      pAddPF += process.step0a
    pAddPF += process.goodOfflinePrimaryVertices
    if useGoodVertex:
      pAddPF += process.step0b
    pAddPF += process.step0c
    pAddPF += process.eidMVASequence
    if useL1FastJet:
      pAddPF += process.ak5PFJets
    if rerunPFTau:
      process.load("RecoTauTag.Configuration.RecoPFTauTag_cff")
      pAddPF += process.PFTau
    pAddPF += process.patDefaultSequence
    pAddPF.remove( process.patJetCorrFactors )
    if useCaloJets and useL1FastJet and useRelVals:
      pAddPF.remove( process.jetTracksAssociatorAtVertex )
      pAddPF.remove( process.impactParameterTagInfosAOD )
      pAddPF.remove( process.secondaryVertexTagInfosAOD )
      pAddPF.remove( process.softMuonTagInfosAOD )
      pAddPF.remove( process.secondaryVertexNegativeTagInfosAOD )
      pAddPF.remove( process.jetBProbabilityBJetTagsAOD )
      pAddPF.remove( process.jetProbabilityBJetTagsAOD )
      pAddPF.remove( process.trackCountingHighPurBJetTagsAOD )
      pAddPF.remove( process.trackCountingHighEffBJetTagsAOD )
      pAddPF.remove( process.simpleSecondaryVertexHighEffBJetTagsAOD )
      pAddPF.remove( process.simpleSecondaryVertexHighPurBJetTagsAOD )
      pAddPF.remove( process.combinedSecondaryVertexBJetTagsAOD )
      pAddPF.remove( process.combinedSecondaryVertexMVABJetTagsAOD )
      pAddPF.remove( process.softMuonBJetTagsAOD )
      pAddPF.remove( process.softMuonByPtBJetTagsAOD )
      pAddPF.remove( process.softMuonByIP3dBJetTagsAOD )
      pAddPF.remove( process.simpleSecondaryVertexNegativeHighEffBJetTagsAOD )
      pAddPF.remove( process.simpleSecondaryVertexNegativeHighPurBJetTagsAOD )
      pAddPF.remove( process.negativeTrackCountingHighEffJetTagsAOD )
      pAddPF.remove( process.negativeTrackCountingHighPurJetTagsAOD )
    pAddPF.remove( process.patJetCharge )
    pAddPF.remove( process.patJetPartonMatch )
    pAddPF.remove( process.patJetGenJetMatch )
    pAddPF.remove( process.patJetPartonAssociation )
    pAddPF.remove( process.patJetFlavourAssociation )
    pAddPF.remove( process.patJets )
    pAddPF.remove( process.patMETs )
    pAddPF.remove( process.selectedPatJets )
    pAddPF.remove( process.countPatJets )
    pAddPF += process.patAddOnSequence
    # pAddPF.replace( process.loosePatMuons
#                   , getattr( process, 'loosePatMuons' + jetAlgo + pfSuffix )
#                   )
#     pAddPF.replace( process.tightPatMuons
#                   , getattr( process, 'tightPatMuons' + jetAlgo + pfSuffix )
#                   )
#     pAddPF.replace( process.goodPatJets
#                   , getattr( process, 'goodPatJets' + jetAlgo + pfSuffix )
#                   )
#    if useLooseMuon:
#      pAddPF += getattr( process, 'step1a' + jetAlgo + pfSuffix )
#    if useTightMuon:
#      pAddPF += getattr( process, 'step1b' + jetAlgo + pfSuffix )
    if useMuonVeto:
      pAddPF += process.step2
    if useElectronVeto:
      pAddPF += process.step3
    if use1Jet:
      pAddPF += getattr( process, 'step4a' + jetAlgo + pfSuffix )
    if use2Jets:
      pAddPF += getattr( process, 'step4b' + jetAlgo + pfSuffix )
#    if use3Jets:
#      pAddPF += getattr( process, 'step4c' + jetAlgo + pfSuffix )
    if use4Jets:
      pAddPF += getattr( process, 'step5' + jetAlgo + pfSuffix )
    setattr( process, 'p' + jetAlgo + pfSuffix, pAddPF )
    process.out.SelectEvents.SelectEvents.append( 'p' + jetAlgo + pfSuffix )

if runPF2PAT:

  pPF = cms.Path()
  if useTrigger:
    pPF += process.step0a
  pPF += process.goodOfflinePrimaryVertices
  if useGoodVertex:
    pPF += process.step0b
  pPF += process.step0c
  pPF += process.eidMVASequence
  #if rerunPFTau:
  #  process.load("RecoTauTag.Configuration.RecoPFTauTag_cff")
  #  pAddPF += process.PFTau
  pPF += getattr( process, 'patPF2PATSequence' + postfix )
  pPF += process.looseLeptonSequence
  pPF += getattr( process, 'patAddOnSequence' + postfix )
  pPF += process.puJetIdSqeuenceChs #Mispelled to match pujetidsequence_cff.py
  pPF += process.patConversions
#  if useLooseMuon:
#    pPF += getattr( process, 'step1a' + postfix )
#  if useTightMuon:
#    pPF += getattr( process, 'step1b' + postfix )
  if useMuonVeto:
    pPF += getattr( process, 'step2' + postfix )
  if useElectronVeto:
    pPF += getattr( process, 'step3' + postfix )
  if use1Jet:
    pPF += getattr( process, 'step4a' + postfix )
  if use2Jets:
    pPF += getattr( process, 'step4b' + postfix )
#  if use3Jets:
#    pPF += getattr( process, 'step4c' + postfix )
  if use4Jets:
    pPF += getattr( process, 'step5' + postfix )
  setattr( process, 'p' + postfix, pPF )
  process.out.SelectEvents.SelectEvents.append( 'p' + postfix )


  #pPF += process.refitMuons
  pPF += process.q2weights
  pPF += process.BNproducer
  setattr( process, 'p' + postfix, pPF )
  process.out.SelectEvents.SelectEvents.append( 'p' + postfix )
  process.out.outputCommands = [ 'drop *' ]
  process.out.outputCommands.extend( [ # BEAN Objects
    'keep *_BNproducer_*_*',
    'keep double_kt6PFJets*_rho_*',
    #'keep *',
    ] )



## Dump python config if wished
#outfile = open('dumpedConfig.py','w'); print >> outfile,process.dumpPython(); outfile.close()
