from collections import defaultdict
import math
import os
import re
import shutil

from lobster import util, fs
from lobster.util import Configurable

__all__ = ['Dataset', 'ParentDataset', 'ProductionDataset']

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
        self.tasksize = 1
        self.total_events = 0
        self.total_lumis = 0
        self.unmasked_lumis = 0
        self.masked_lumis = 0

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
            How many files to process in one task
    """
    _mutable = []

    def __init__(self, files, files_per_task=1):
        self.files = files
        self.files_per_task = files_per_task
        self.total_units = 0

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = True

        dset.tasksize = self.files_per_task
        if not isinstance(self.files, list):
            self.files = [self.files]
        files = []
        for entry in self.files:
            entry = os.path.expanduser(entry)
            if fs.isdir(entry):
                files += filter(fs.isfile, fs.ls(entry))
            elif fs.isfile(entry):
                files.append(entry)
        dset.total_lumis = len(files)
        self.total_units = len(files)

        for fn in files:
            # hack because it will be slow to open all the input files to read the run/lumi info
            dset.files[fn].lumis = [(-1, -1)]
            dset.files[fn].size = fs.getsize(fn)

        return dset

class ProductionDataset(Configurable):
    """
    Dataset specification for Monte-Carlo event generation.

    Parameters
    ----------
        events_per_task : int
            How many events to generate in one task.
        events_per_lumi : int
            How many events should be in one luminosity section
        numbor_of_tasks : int
            How many tasks to run.
        randomize_seeds : bool
            Use random seeds every time a task is run.
    """
    _mutable = []
    def __init__(self, events_per_task, events_per_lumi=None, number_of_tasks=1, randomize_seeds=True):
        self.number_of_tasks = number_of_tasks
        self.events_per_task = events_per_task
        self.events_per_lumi = events_per_lumi
        self.randomize_seeds = randomize_seeds

        nlumis = 1
        if events_per_lumi:
            nlumis = int(math.ceil(float(events_per_task) / events_per_lumi))
        self.total_units = number_of_tasks * nlumis

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = True

        ntasks = self.number_of_tasks
        nlumis = 1
        if self.events_per_lumi:
            nlumis = int(math.ceil(float(self.events_per_task) / self.events_per_lumi))
        dset.files[None].lumis = [(1, x) for x in range(1, ntasks * nlumis + 1, nlumis)]
        dset.total_lumis = ntasks
        self.total_units = ntasks * nlumis

        return dset

class ParentDataset(Configurable):
    """
    Process the output of another workflow.

    Parameters
    ----------
        parent : Dataset
            The parent dataset to process.
        units_per_task : int
            How much of the parent dataset to process at once.  Can be
            changed by Lobster to match the user-specified task runtime.
    """
    _mutable = []
    def __init__(self, parent, units_per_task=1):
        self.parent = parent
        self.units_per_task = units_per_task
        self.total_units = self.parent.total_units

    def get_info(self):
        # in case the parent object gets updated in the meantime
        self.total_units = self.parent.total_units

        dset = DatasetInfo()
        dset.file_based = False
        dset.tasksize = self.units_per_task
        dset.total_lumis = self.total_units

        return dset
