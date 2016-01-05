from collections import defaultdict
import math
import os
import re
import shutil
from lobster import util, fs

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

class Dataset(object):
    def __init__(self, files, files_per_task=1):
        self.files = files
        self.files_per_task = files_per_task
        self.total_units = 0

    def get_info(self):
        dset = DatasetInfo()
        dset.file_based = True

        dset.tasksize = self.files_per_task
        if not isinstance(files, list):
            files = [files]
        for entry in files:
            entry = os.path.expanduser(entry)
            if fs.isdir(entry):
                files = filter(fs.isfile, fs.ls(entry))
        dset.total_lumis = len(files)
        self.total_units = len(files)

        for fn in files:
            # hack because it will be slow to open all the input files to read the run/lumi info
            dset.files[fn].lumis = [(-1, -1)]
            dset.files[fn].size = fs.getsize(fn)

        return dset

class ProductionDataset(object):
    def __init__(self, events_per_task, events_per_lumi=None, number_of_tasks=1, randomize_seeds=True):
        self.number_of_tasks = number_of_tasks
        self.events_per_task = events_per_task
        self.events_per_lumi = events_per_lumi

        nlumis = 1
        if events_per_lumi:
            nlumis = int(math.ceil(float(events_per_task) / events_per_lumi))
        self.total_units = number_of_tasks * nlumis

    def get_info(self):
        dset = DatasetInfo()
        dest.file_based = True

        ntasks = self.number_of_tasks
        nlumis = 1
        if self.events_per_lumi:
            nlumis = int(math.ceil(float(self.events_per_task) / self.events_per_lumi))
        dset.files[None].lumis = [(1, x) for x in range(1, ntasks * nlumis + 1, nlumis)]
        dset.total_lumis = ntasks
        self.total_units = ntasks * nlumis

        return dset

class ParentDataset(object):
    def __init__(self, wflow, units_per_task=1):
        self.parent = wflow.dataset
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
