import datetime

from lobster import cmssw
from lobster.core import *

version = datetime.datetime.now().strftime('%Y%m%d')

storage = StorageConfiguration(
    output=[
        "hdfs:///store/user/$USER/lobster_test_" + version,
        "file:///hadoop/store/user/$USER/lobster_test_" + version,
        # ND is not in the XrootD redirector, thus hardcode server.
        # Note the double-slash after the hostname!
        "root://deepthought.crc.nd.edu//store/user/$USER/lobster_test_" + version,
        "chirp://eddie.crc.nd.edu:9094/store/user/$USER/lobster_test_" + version,
        "srm://T3_US_NotreDame/store/user/$USER/lobster_test_" + version
    ]
)

processing = Category(
    name='processing',
    cores=1,
    runtime=900,
    memory=1000
)

workflows = []

single_mu = Workflow(
    label='single_mu',
    dataset=cmssw.Dataset(
        dataset='/SingleMu/Run2012A-recover-06Aug2012-v1/AOD',
        events_per_task=5000
    ),
    category=processing,
    pset='slim.py',
    publish_label='test',
    merge_size='3.5G',
    outputs=['output.root']
)

workflows.append(single_mu)

config = Config(
    workdir='/tmpscratch/users/$USER/lobster_test_' + version,
    plotdir='~/www/lobster/test_' + version,
    storage=storage,
    workflows=workflows,
    advanced=AdvancedOptions(log_level=1)
)
