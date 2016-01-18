import os
import pickle
import re

import lobster.cmssw as cmssw
from lobster.core import Dataset, ParentDataset, ProductionDataset, Category, Workflow
from lobster.se import StorageConfiguration

def apply_matching(config):
    if 'workflow defaults' not in config:
        return config
    defaults = config['workflow defaults']
    matching = defaults.get('matching', [])
    configs = []

    for cfg in config['workflows']:
        label = cfg['label']

        for match in matching:
            if re.search(match['label'], label):
                for k, v in match.items():
                    if k == 'label':
                        continue
                    if k not in cfg:
                        cfg[k] = v
        for k, v in defaults.items():
            if k == 'matching':
                continue
            if k not in cfg:
                cfg[k] = v

        configs.append(cfg)

    config['workflows'] = configs
    del config['workflow defaults']

    return config


def extract_category(config):
    category = Category(
            name=config.pop('category', config['label']),
            cores=config.pop('cores per task', 1),
            disk=config.pop('task disk', None),
            memory=config.pop('task memory', None),
            runtime=config.pop('task runtime', None)
    )
    return category


def extract_dataset(config):
    if 'dataset' in config:
        cls = cmssw.Dataset
        dset_kwargs = {
                'dataset': None,
                'lumi mask': None,
                'lumis per task': 25,
                'events per task': None,
                'file based': False,
                'dbs instance': 'global',
        }
    elif 'files' in config:
        cls = Dataset
        dset_kwargs = {
                'files': [],
                'files per task': 1
        }
    elif 'events per task' in config:
        cls = ProductionDataset
        dset_kwargs = {
                'events per task': None,
                'events per lumi': None,
                'number of tasks': 1,
                'randomize seeds': True
        }

        if 'num tasks' in config:
            config['number of tasks'] = config.pop('num tasks')
    elif 'parent' in config:
        cls = ParentDataset
        dset_kwargs = {
                'parent': None,
                'units per task': 1
        }
    else:
        raise NotImplementedError("can't extract a dataset out of: " + repr(config))

    for key in dset_kwargs.keys():
        if key in config:
            dset_kwargs[key] = config.pop(key)
        else:
            del dset_kwargs[key]

    return cls(**pythonize_keys(dset_kwargs))


def pythonize_keys(config):
    return dict([(k.replace(" ", "_").replace("-", "_"), v) for k, v in config.items()])


def pythonize_yaml(config):
    config = apply_matching(config)

    config['advanced'] = AdvancedOptions(**pythonize_keys(config['advanced']))
    config['storage'] = StorageConfiguration(**pythonize_keys(config['storage']))
    config['label'] = config.pop('id')

    workflows = []
    for cfg in config['workflows']:
        if 'parent dataset' in cfg:
            name = cfg.pop('parent dataset')
            for w in workflows:
                if w.label == name:
                    cfg['parent'] = w.dataset
                    break
            else:
                raise ValueError("parent {0} not defined in configuration before usage".format(name))

        if 'delete merged' in cfg:
            cfg['merge_cleanup'] = cfg.pop('delete merged')

        if 'lumis per task' in cfg:
            cfg['units per task'] = cfg.pop('lumis per task')

        cfg['dataset'] = extract_dataset(cfg)
        cfg['category'] = extract_category(cfg)

        workflows.append(Workflow(**pythonize_keys(cfg)))
    config['workflows'] = workflows

    return Config(**pythonize_keys(config))


class Config(object):
    """Top level Lobster configuration object
    """
    def __init__(self, label, workdir, storage, workflows, advanced=None, plotdir=None,
            foremen_logs=None,
            base_directory=None, base_configuration=None, startup_directory=None):
        self.label = label
        self.workdir = workdir
        self.plotdir = plotdir
        self.foremen_logs = foremen_logs
        self.storage = storage
        self.workflows = workflows
        self.advanced = advanced if advanced else AdvancedOptions()

        self.base_directory = base_directory
        self.base_configuration = base_configuration
        self.startup_directory = startup_directory

    @classmethod
    def load(cls, path):
        try:
            with open(os.path.join(path, 'config.pkl'), 'rb') as f:
                return pickle.load(f)
        except IOError:
            raise IOError("can't load configuration from {0}".format(os.path.join(path, 'config.pkl')))

    def save(self):
        with open(os.path.join(self.workdir, 'config.pkl'), 'wb') as f:
            pickle.dump(self, f)


class AdvancedOptions(object):
    """Advanced options for tuning Lobster
    """
    def __init__(self,
            use_dashboard=True,
            abort_threshold=10,
            abort_multiplier=4,
            bad_exit_codes=None,
            dump_core=False,
            full_monitoring=False,
            log_level=2,
            payload=10,
            renew_proxy=True,
            threshold_for_failure=30,
            threshold_for_skipping=30,
            wq_max_retries=10):
        self.use_dashboard = use_dashboard
        self.abort_threshold = abort_threshold
        self.abort_multiplier = abort_multiplier
        self.bad_exit_codes = bad_exit_codes if bad_exit_codes else [169]
        self.dump_core = dump_core
        self.full_monitoring = full_monitoring
        self.log_level = log_level
        self.payload = payload
        self.renew_proxy = renew_proxy
        self.threshold_for_failure = threshold_for_failure
        self.threshold_for_skipping = threshold_for_skipping
        self.wq_max_retries = wq_max_retries
