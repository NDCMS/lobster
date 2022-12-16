import datetime

from lobster import cmssw
from lobster.core import AdvancedOptions, Category, Config, Dataset, ParentDataset, StorageConfiguration, Workflow

version = datetime.datetime.now().strftime('%Y%m%d_%H%M')
input_path="/store/user/"

storage = StorageConfiguration(
    input=[
        "root://deepthought.crc.nd.edu/" + input_path,  # Note the extra slash after the hostname!
        "hdfs://eddie.crc.nd.edu:19000"  + input_path,
        "gsiftp://T3_US_NotreDame"       + input_path,
        "srm://T3_US_NotreDame"          + input_path,
    ],
    output=[
        "file:///hadoop/store/user/$USER/lobster_test_" + version,
        "hdfs://eddie.crc.nd.edu:19000/store/user/$USER/lobster_test_" + version,
        # ND is not in the XrootD redirector, thus hardcode server.
        # Note the double-slash after the hostname!
        "root://deepthought.crc.nd.edu//store/user/$USER/lobster_test_" + version,
        "gsiftp://T3_US_NotreDame/store/user/$USER/lobster_test_" + version,
        "srm://T3_US_NotreDame/store/user/$USER/lobster_test_" + version
    ]
)

processing = Category(
    name='processing',
    cores=1,
    runtime=900,
    memory=1000
)

wf = []

#data_dir = "/kmohrman/FullProduction/FullR2/UL17/Round1/Batch1/postLHE_step/v2/mAOD_step_ttHJet_all22WCsStartPtCheckdim6TopMay20GST_run0/"
data_dir = "hnelson2/testDatasets/"

ttH = Workflow(
    label='ttH',
#    dataset=cmssw.Dataset(
#        dataset='/ttHJetToNonbb_M125_TuneCP5_13TeV_amcatnloFXFX_madspin_pythia8/RunIISummer20UL18NanoAODv9-106X_upgrade2018_realistic_v16_L1v1-v1/NANOAODSIM',
#        events_per_task=50000
#    ),

    command='cmsRun simple_pset.py',
    sandbox=cmssw.Sandbox(release='CMSSW_10_6_26'),
    merge_size='3.5G',
    dataset=Dataset(
        files=data_dir,
        files_per_task=1,
        patterns=["*.root"],
     #   total_events=10000
    ),
    category=processing,
    outputs=['output.root']
)

wf.append(ttH)

config = Config(
    workdir='/tmpscratch/users/$USER/lobster_test_' + version,
    plotdir='~/www/lobster/test_' + version,
    storage=storage,
    workflows=wf,
    advanced=AdvancedOptions(
        bad_exit_codes=[127, 160],
        log_level=1,
        dashboard=False,
        #wq_port=[9123,9129]
    )
)
