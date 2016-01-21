import lobster.cmssw as cmssw
from lobster.core import *

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

    if 'advanced' in config:
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
