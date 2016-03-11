# Producing MC à la MCM

Go to MCM for [some random workflow][mcm].  Do the following to download
the steps:

    curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIIWinter15wmLHE-00196 > setup01_lhe.sh
    curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIIWinter15GS-00301 > setup02_gs.sh
    curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIISpring15DR74-00280 > setup03_dr.sh
    curl -k https://cms-pdmv.cern.ch/mcm/public/restapi/requests/get_setup/HIG-RunIISpring15MiniAODv2-00169 > setup04_v4.sh

Remove any dependency on CERN's AFS:

    sed -i '/source  \/afs/d' *.sh

Execute the setup for every step:

    for f in *.sh; do sh $f; done

I had to fix the input source for the GEN-SIM step, since the configuration
produced by the above steps contains an `EmptySource`, but should really be
a `PoolSource` to run on the first step.

Create a Lobster configuration like this:

    id: mate
    type: cmssw

    workdir: /tmpscratch/users/matze/prod_v3
    plotdir: /afs/crc.nd.edu/user/m/mwolf3/www/lobster/prod_v3

    storage:
        output:
          - file:///hadoop/store/user/matze/prod/v3
          - hdfs:///store/user/matze/prod/v3
          - root://ndcms.crc.nd.edu//store/user/matze/prod/v3
          - srm://T3_US_NotreDame/store/user/matze/prod/v3

    use dashboard: true

    advanced:
      log level: 0

    workflows:
      - label: lhe_step
        cmssw config: HIG-RunIIWinter15wmLHE-00196_1_cfg.py
        outputs: [HIG-RunIIWinter15wmLHE-00196.root]
        sandbox release: /afs/crc.nd.edu/user/m/mwolf3/work/ttH/mc_gen2/CMSSW_7_1_16_patch1
        events per task: 4000
        events per lumi: 200
        num tasks: 1000
        cores per task: 2

      - label: gs_step
        parent dataset: lhe_step
        cmssw config: HIG-RunIIWinter15GS-00301_1_cfg.py
        outputs: [HIG-RunIIWinter15GS-00301.root]
        sandbox release: /afs/crc.nd.edu/user/m/mwolf3/work/ttH/mc_gen2/CMSSW_7_1_16_patch2
        lumis per task: 1
        task runtime: 3600
        cores per task: 4

      - label: digi_step
        parent dataset: gs_step
        cmssw config: HIG-RunIISpring15DR74-00280_1_cfg.py
        outputs: [HIG-RunIISpring15DR74-00280_step1.root]
        sandbox release: /afs/crc.nd.edu/user/m/mwolf3/work/ttH/mc_gen2/CMSSW_7_4_1_patch4
        lumis per task: 10
        task runtime: 3600
        cores per task: 4

      - label: reco_step
        parent dataset: digi_step
        cmssw config: HIG-RunIISpring15DR74-00280_2_cfg.py
        outputs: [HIG-RunIISpring15DR74-00280_step2.root]
        sandbox release: /afs/crc.nd.edu/user/m/mwolf3/work/ttH/mc_gen2/CMSSW_7_4_1_patch4
        lumis per task: 1
        task runtime: 3600
        cores per task: 4

      - label: mAOD_step
        parent dataset: reco_step
        cmssw config: HIG-RunIISpring15MiniAODv2-00169_1_cfg.py
        outputs: [HIG-RunIISpring15MiniAODv2-00169.root]
        sandbox release: /afs/crc.nd.edu/user/m/mwolf3/work/ttH/mc_gen2/CMSSW_7_4_14
        lumis per task: 60
        task runtime: 3600
        cores per task: 1

[mcm]: https://cms-pdmv.cern.ch/mcm/requests?dataset_name=ttHToTT_M125_13TeV_powheg_pythia8&page=0&shown=127
