from lobster import cmssw
from lobster.core import *

version = '_take8'

storage = StorageConfiguration(
        output=[
            "hdfs:///store/user/matze/test_mc" + version,
            "file:///hadoop/store/user/matze/test_mc" + version,
            "chirp://earth:9666/test_mc" + version,
            "root://deepthought.crc.nd.edu//store/user/matze/test_mc" + version,
            "srm://T3_US_NotreDame/store/user/matze/test_mc" + version,
        ]
)

workflows = []

lhe = Workflow(
        label='lhe_step',
        pset='mc_gen/HIG-RunIIWinter15wmLHE-00196_1_cfg.py',
        sandbox_release='mc_gen/CMSSW_7_1_16_patch1',
        merge_size='3.5G',
        dataset=ProductionDataset(
            events_per_task=500,
            events_per_lumi=50,
            number_of_tasks=1000
        ),
        category=Category(
            name='lhe',
            cores=2,
            memory=2000
        )
)

gs = Workflow(
        label='gs_step',
        pset='mc_gen/HIG-RunIIWinter15GS-00301_1_cfg.py',
        sandbox_release='mc_gen/CMSSW_7_1_16_patch2',
        merge_size='3.5G',
        dataset=ParentDataset(
            parent=lhe.dataset,
            units_per_task=1
        ),
        category=Category(
            name='gs',
            cores=4,
            memory=2000
        )
)

digi = Workflow(
        label='digi_step',
        pset='mc_gen/HIG-RunIISpring15DR74-00280_1_cfg.py',
        sandbox_release='mc_gen/CMSSW_7_4_1_patch4',
        merge_size='3.5G',
        dataset=ParentDataset(
            parent=gs.dataset,
            units_per_task=10
        ),
        category=Category(
            name='digi',
            cores=4,
            memory=2600,
            runtime=7200
        )
)

reco = Workflow(
        label='reco_step',
        pset='mc_gen/HIG-RunIISpring15DR74-00280_2_cfg.py',
        sandbox_release='mc_gen/CMSSW_7_4_1_patch4',
        merge_size='3.5G',
        dataset=ParentDataset(
            parent=digi.dataset,
            units_per_task=6
        ),
        category=Category(
            name='reco',
            cores=4,
            memory=2800,
            runtime=7200
        )
)

maod = Workflow(
        label='mAOD_step',
        pset='mc_gen/HIG-RunIISpring15MiniAODv2-00169_1_cfg.py',
        sandbox_release='mc_gen/CMSSW_7_4_14',
        merge_size='3.5G',
        dataset=ParentDataset(
            parent=reco.dataset,
            units_per_task=60
        ),
        category=Category(
            name='mAOD',
            cores=1,
            memory=2000,
            runtime=3600
        )
)

config = Config(
        label='shuffle',
        workdir='/tmpscratch/users/matze/test_mc' + version,
        plotdir='/afs/crc.nd.edu/user/m/mwolf3/www/lobster/test_mc' + version,
        storage=storage,
        workflows=[lhe, gs, digi, reco, maod],
        advanced=AdvancedOptions(log_level=1)
)
