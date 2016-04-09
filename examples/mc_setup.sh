#!/bin/sh

set -e

mkdir -p mc_gen
cd mc_gen

source /cvmfs/cms.cern.ch/cmsset_default.sh

curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIIWinter15wmLHE-00196 > setup01_lhe.sh
curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIISummer15GS-00177 > setup02_gs.sh
curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIIFall15DR76-00243 > setup03_dr.sh
curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIIFall15MiniAODv2-00224 > setup04_v4.sh

sed -i 's@/afs/.*@/cvmfs/cms.cern.ch/cmsset_default.sh@g' setup*.sh
sed -i 's@export X509.*@@' setup*.sh

for f in *.sh; do
	sh $f;
done

cat <<EOF >> HIG-RunIIWinter15wmLHE-00196_1_cfg.py
process.maxEvents.input = cms.untracked.int32(1)
process.externalLHEProducer.nEvents = cms.untracked.uint32(1)
EOF

cat <<EOF >> HIG-RunIISummer15GS-00177_1_cfg.py
process.source = cms.Source("PoolSource", fileNames = cms.untracked.vstring('file:HIG-RunIIWinter15wmLHE-00196.root'))
EOF

cat <<EOF >> HIG-RunIIFall15DR76-00243_1_cfg.py
process.source.fileNames = cms.untracked.vstring('file:HIG-RunIISummer15GS-00177.root')
EOF

cat <<EOF >> HIG-RunIIFall15MiniAODv2-00224_1_cfg.py
process.source.fileNames = cms.untracked.vstring('file:HIG-RunIIFall15DR76-00243.root')
EOF

if [ -n "$RUN_MC" ]; then
	cd CMSSW_7_1_16_patch1/; cmsenv; cd -
	cmsRun -n 4 HIG-RunIIWinter15wmLHE-00196_1_cfg.py

	cd CMSSW_7_1_18/; cmsenv; cd -
	cmsRun -n 4 HIG-RunIISummer15GS-00177_1_cfg.py

	cd CMSSW_7_6_1/; cmsenv; cd -
	cmsRun -n 4 HIG-RunIIFall15DR76-00243_1_cfg.py
	cmsRun -n 4 HIG-RunIIFall15DR76-00243_2_cfg.py

	cd CMSSW_7_6_3/; cmsenv; cd -
	cmsRun -n 4 HIG-RunIIFall15MiniAODv2-00224_1_cfg.py
fi
