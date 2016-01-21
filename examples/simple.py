from lobster import cmssw
from lobster.core import *

storage = StorageConfiguration(
        output=[
            "hdfs:///store/user/matze/test_shuffle_take29",
            "file:///hadoop/store/user/matze/test_shuffle_take29",
            "root://ndcms.crc.nd.edu//store/user/matze/test_shuffle_take29",
            "srm://T3_US_NotreDame/store/user/matze/test_shuffle_take29",
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
        cmssw_config='slim.py',
        publish_label='test',
        merge_size='3.5G',
        outputs=['output.root']
)

workflows.append(single_mu)

config = Config(
        label='shuffle',
        workdir='/tmpscratch/users/matze/test_shuffle_take29',
        plotdir='/afs/crc.nd.edu/user/m/mwolf3/www/lobster/test_shuffle_take29',
        storage=storage,
        workflows=workflows,
        advanced=AdvancedOptions(log_level=1)
)
