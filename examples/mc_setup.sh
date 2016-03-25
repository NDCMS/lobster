#!/bin/sh

set -e

mkdir -p mc_gen
cd mc_gen

source /cvmfs/cms.cern.ch/cmsset_default.sh

curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIIWinter15wmLHE-00196 > setup01_lhe.sh
curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIIWinter15GS-00301 > setup02_gs.sh
curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIISpring15DR74-00280 > setup03_dr.sh
curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIISpring15MiniAODv2-00169 > setup04_v4.sh

sed -i s@/afs/.*@/cvmfs/cms.cern.ch/cmsset_default.sh@g setup*.sh

for f in *.sh; do
	sh $f;
done

cat <<EOF >> HIG-RunIIWinter15wmLHE-00196_1_cfg.py
process.maxEvents.input = cms.untracked.int32(1)
process.externalLHEProducer.nEvents = cms.untracked.uint32(1)
EOF

cat <<EOF >> HIG-RunIIWinter15GS-00301_1_cfg.py
process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring('file:HIG-RunIIWinter15wmLHE-00196.root'))
EOF

cat <<EOF >> HIG-RunIISpring15DR74-00280_1_cfg.py
process.source.fileNames = cms.untracked.vstring('file:HIG-RunIIWinter15GS-00301.root')
EOF

cat <<EOF >> HIG-RunIISpring15MiniAODv2-00169_1_cfg.py
process.source.fileNames = cms.untracked.vstring('file:HIG-RunIISpring15DR74-00280_step2.root')
EOF

if [ -n "$RUN_MC" ]; then
	cd CMSSW_7_1_16_patch1/; cmsenv; cd -
	cmsRun -n 4 HIG-RunIIWinter15wmLHE-00196_1_cfg.py

	cd CMSSW_7_1_16_patch2/; cmsenv; cd -
	cmsRun -n 4 HIG-RunIIWinter15GS-00301_1_cfg.py

	cd CMSSW_7_4_1_patch4/; cmsenv; cd -
	cmsRun -n 4 HIG-RunIISpring15DR74-00280_1_cfg.py
	cmsRun -n 4 HIG-RunIISpring15DR74-00280_2_cfg.py

	cd CMSSW_7_4_14/; cmsenv; cd -
	cmsRun -n 4 HIG-RunIISpring15MiniAODv2-00169_1_cfg.py
fi
