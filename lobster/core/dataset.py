from collections import defaultdict
import fnmatch
import math
import os

from lobster import fs
from lobster.util import Configurable

__all__ = [
    'Dataset', 'EmptyDataset', 'ParentDataset', 'ProductionDataset',
    'MultiGridpackDataset', 'ParentMultiGridpackDataset', 'MultiProductionDataset'
]


def flatten(files, matches=None):
    """Flatten a list of directories or files to a single list of files.

    Parameters
    ----------
        files : str or list
            A list of paths to expand. Can also be a string containing a path.
        matches : list
            A list of patterns to match files against. Only successfully
            matched files will be returned.

    Returns
    -------
        files : list
            A list of files found in the paths passed in the input
            parameter `files`, optionally matching the extensions in
            `exts`.
    """
    def matchfn(fn):
        base = os.path.basename(fn)
        for m in matches:
            if fnmatch.fnmatch(base, m):
                return True
        return False
    res = []
    if not isinstance(files, list):
        files = [files]
    for entry in files:
        entry = os.path.expanduser(entry)
        if fs.isdir(entry):
            res.extend(fs.ls(entry))
        elif fs.isfile(entry):
            res.append(entry)
    if matches:
        return [fn for fn in res if matchfn(fn)]
    return res


class FileInfo(object):

    def __init__(self):
        self.lumis = []
        self.events = 0
        self.size = 0

    def __repr__(self):
        descriptions = ['{a}={v}'.format(a=attribute, v=getattr(self, attribute)) for attribute in self.__dict__]
        return 'FileInfo({0})'.format(',\n'.join(descriptions))


class DatasetInfo(object):

    def __init__(self):
        self.file_based = False
        self.files = defaultdict(FileInfo)
        self.stop_on_file_boundary = False
        self.tasksize = 1
        self.total_events = 0
        self.total_units = 0
        self.unmasked_units = 0
        self.masked_units = 0

    def __repr__(self):
        descriptions = ['{a}={v}'.format(a=attribute, v=getattr(self, attribute)) for attribute in self.__dict__]
        return 'DatasetInfo({0})'.format(',\n'.join(descriptions))


class Dataset(Configurable):

    """
    A simple dataset specification.

    Runs over files found in a list of directories or specified directly.

    Parameters
    ----------
        files : list
            A list of files or directories to process.  May also be a `str`
            pointing to a single file or directory.
        files_per_task : int
            How many files to process in one task. Defaults to 1.
        patterns: list
            A list of shell-style file patterns to match filenames against.
            Defaults to `None` and will use all files considered.
    """
    _mutable = {}

    def __init__(self, files, files_per_task=1, patterns=None):
        self.files = files
        self.files_per_task = files_per_task
        self.patterns = patterns
        self.total_units = 0

    def validate(self):
        return len(flatten(self.files, self.patterns)) > 0

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = True

        files = flatten(self.files, self.patterns)
        dset.tasksize = self.files_per_task
        dset.total_units = len(files)
        self.total_units = len(files)

        for fn in files:
            # hack because it will be slow to stat and open
            # all the input files to read the size/run/lumi info
            dset.files[fn].lumis = [(-1, -1)]

        return dset


class EmptyDataset(Configurable):

    """
    Dataset specification for non-cmsRun workflows with no input files.

    Parameters
    ----------
        number_of_tasks : int
            How many tasks to run.
    """
    _mutable = {}

    def __init__(self, number_of_tasks=1):
        self.number_of_tasks = number_of_tasks
        self.total_units = number_of_tasks

    def validate(self):
        return True

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = True

        dset.files[None].lumis = [(x, 1) for x in range(1, self.number_of_tasks + 1)]
        dset.total_units = self.total_units

        return dset


class ProductionDataset(Configurable):

    """
    Dataset specification for Monte-Carlo event generation.

    Parameters
    ----------
        total_events : int
            How many events to generate.
        events_per_lumi : int
            How many events to generate in one luminosity section.
        lumis_per_task : int
            How many lumis to produce per task. Can be
            changed by Lobster to match the user-specified task runtime.
        randomize_seeds : bool
            Use random seeds every time a task is run.
    """
    _mutable = {}

    def __init__(self, total_events, events_per_lumi=500, lumis_per_task=1, randomize_seeds=True):
        self.total_events = total_events
        self.events_per_lumi = events_per_lumi
        self.lumis_per_task = lumis_per_task
        self.randomize_seeds = randomize_seeds

        self.total_units = int(math.ceil(float(total_events) / events_per_lumi))

    def validate(self):
        return True

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = True

        dset.files[None].lumis = [(1, x) for x in range(1, self.total_units + 1)]
        dset.total_units = self.total_units
        dset.tasksize = self.lumis_per_task

        return dset


class MultiProductionDataset(ProductionDataset):

    """
    Dataset specification for Monte-Carlo event generation from a set
    of gridpacks.

    Parameters
    ----------
        gridpacks : list
            A list of gridpack files or directories to process.  May also be a `str`
            pointing to a single gridpack file or directory.
        events_per_gridpack : int
            How many events to generate per gridpack.
        events_per_lumi : int
            How many events to generate in one luminosity section.
        lumis_per_task : int
            How many lumis to produce per task. Can be
            changed by Lobster to match the user-specified task runtime.
        randomize_seeds : bool
            Use random seeds every time a task is run.
    """
    _mutable = {}

    def __init__(self, gridpacks, events_per_gridpack, events_per_lumi=500, lumis_per_task=1, randomize_seeds=True):
        self.gridpacks = gridpacks
        self.events_per_gridpack = events_per_gridpack
        self.events_per_lumi = events_per_lumi
        self.lumis_per_task = lumis_per_task
        self.randomize_seeds = randomize_seeds

        self.lumis_per_gridpack = int(math.ceil(float(events_per_gridpack) / events_per_lumi))
        self.total_units = len(flatten(self.gridpacks)) * self.lumis_per_gridpack

    def validate(self):
        return len(flatten(self.gridpacks)) > 0

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = True

        for run, fn in enumerate(flatten(self.gridpacks)):
            dset.files[fn].lumis = [(run, x) for x in range(1, self.lumis_per_gridpack + 1)]

        dset.total_units = self.total_units
        dset.tasksize = self.lumis_per_task
        dset.stop_on_file_boundary = True  # CMSSW can only handle one gridpack at a time

        return dset


class MultiGridpackDataset(Configurable):
    """

    Dataset specification for producing a set of gridpacks.

    Parameters
    ----------
        events_per_gridpack : int
            If used with a subsequent ParentMultiGridpackDataset,
            how many events to generate per gridpack.
        events_per_lumi : int
            If used with a subsequent ParentMultiGridpackDataset,
            how many events that dataset will have per luminosity section.
    """
    _mutable = {}

    def __init__(self, events_per_gridpack, events_per_lumi):
        self.events_per_gridpack = events_per_gridpack
        self.events_per_lumi = events_per_lumi
        self.lumis_per_gridpack = int(math.ceil(float(events_per_gridpack) / events_per_lumi))
        self.total_units = 1

    def validate(self):
        return True

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = True

        dset.files[None].lumis = [(1, 1)]
        dset.total_units = 1

        return dset


class ParentMultiGridpackDataset(ProductionDataset):
    """

    Dataset specification for Monte-Carlo event generation from a
    parent workflow which produces a set of gridpacks.

    Parameters
    ----------
        parent : Workflow
            The parent workflow to process. The parent workflow should
            process a MultiGridpackDataset.
        randomize_seeds : bool
            Use random seeds every time a task is run.
        units_per_task : int
            How much of the parent dataset to process at once.  Can be
            changed by Lobster to match the user-specified task runtime.
    """
    _mutable = {}

    def __init__(self, parent, randomize_seeds=True, units_per_task=1):
        self.parent = parent
        self.randomize_seeds = randomize_seeds
        self.units_per_task = units_per_task
        self.events_per_lumi = parent.dataset.events_per_lumi

        numgridpacks = len(self.parent.unique_arguments)
        self.total_units = parent.dataset.lumis_per_gridpack * numgridpacks

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = False
        dset.stop_on_file_boundary = True  # CMSSW can only handle one gridpack at a time
        dset.total_units = self.total_units
        dset.tasksize = self.units_per_task

        return dset


class ParentDataset(Configurable):

    """
    Process the output of another workflow.

    Parameters
    ----------
        parent : Workflow
            The parent workflow to process.
        units_per_task : int
            How much of the parent dataset to process at once.  Can be
            changed by Lobster to match the user-specified task runtime.
    """
    _mutable = {}

    def __init__(self, parent, units_per_task=1):
        self.parent = parent
        self.units_per_task = units_per_task
        self.total_units = self.parent.dataset.total_units

    def __repr__(self):
        override = {'parent': 'workflow_' + self.parent.label}
        return Configurable.__repr__(self, override)

    def validate(self):
        return True

    def get_info(self):
        # in case the parent object gets updated in the meantime
        self.total_units = self.parent.dataset.total_units

        dset = DatasetInfo()
        dset.file_based = False
        dset.tasksize = self.units_per_task
        dset.total_units = self.total_units

        return dset
