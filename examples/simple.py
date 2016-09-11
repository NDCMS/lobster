import datetime

from lobster import cmssw
from lobster.core import AdvancedOptions, Category, Config, StorageConfiguration, Workflow

from lobster import datasets

version = datetime.datetime.now().strftime('%Y%m%d_%H%M_stress_')

storage = StorageConfiguration(
    output=[
        "hdfs:///store/user/$USER/lobster_test_" + version,
        "file:///hadoop/store/user/$USER/lobster_test_" + version,
        # ND is not in the XrootD redirector, thus hardcode server.
        # Note the double-slash after the hostname!
        "root://deepthought.crc.nd.edu//store/user/$USER/lobster_test_" + version,
        "chirp://eddie.crc.nd.edu:9094/store/user/$USER/lobster_test_" + version,
        "gsiftp://T3_US_NotreDame/store/user/$USER/lobster_test_" + version,
        "srm://T3_US_NotreDame/store/user/$USER/lobster_test_" + version
    ],
    input=[
        # "hdfs:///store/user/$USER/lobster_test_" + version,
        # "file:///hadoop/store/user/$USER/lobster_test_" + version,
        # ND is not in the XrootD redirector, thus hardcode server.
        # Note the double-slash after the hostname!
        "root://ndcms.crc.nd.edu//",
        # "chirp://eddie.crc.nd.edu:9094/store/user/$USER/lobster_test_" + version,
        # "gsiftp://T3_US_NotreDame/store/user/$USER/lobster_test_" + version,
        # "srm://T3_US_NotreDame/store/user/$USER/lobster_test_" + version
    ],
    disable_input_streaming=True
)

processing = Category(
    name='processing',
    cores=1,
    runtime=900,
    # memory=1000
)

workflows = []


# for path in datasets.datasets('all'):
for label, path in [('HIMinimumBias2', '/HIMinimumBias2/HIRun2015-02May2016-v1/AOD'), ('HIMinimumBias5', '/HIMinimumBias5/HIRun2015-02May2016-v1/AOD')]:
    # _, major, minor, _ = path.split('/')
    # minor = datasets.mctag.sub('', minor)
    # label = (major + '_' + minor).replace('-', '_')
    workflows.append(Workflow(
        label=label,
        dataset=cmssw.Dataset(
            dataset=path,
            events_per_task=135000,
            file_based=True
        ),
        category=processing,
        pset='slim.py',
        publish_label='test',
        outputs=[]
        )
    )

config = Config(
    label=version,
    workdir='/tmpscratch/users/$USER/lobster_test_' + version,
    plotdir='~/www/lobster/test_' + version,
    storage=storage,
    workflows=workflows,
    advanced=AdvancedOptions(log_level=1)
    xrootd_servers=['ndcms.crc.nd.edu']
)
