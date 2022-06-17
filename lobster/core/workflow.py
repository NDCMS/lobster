# -*- coding: utf-8 -*-
import imp
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys

from lobster import fs, util
from lobster.core.dataset import EmptyDataset, MultiGridpackDataset, ParentMultiGridpackDataset, MultiProductionDataset, ProductionDataset
from lobster.core.task import MergeTaskHandler, MultiGridpackTaskHandler, MultiProductionTaskHandler, ProductionTaskHandler, TaskHandler
from lobster.util import Configurable

import work_queue as wq

logger = logging.getLogger('lobster.workflow')


class Category(Configurable):

    """
    Resource specification for one or more
    :class:`~lobster.core.workflow.Workflow`.

    This information will be passed on to `WorkQueue`, which will forcibly
    terminate tasks of :class:`.Workflow` in the group that exceed the
    specified resources.

    Attributs modifiable at runtime:

    * `tasks_min`
    * `tasks_max`
    * `runtime`

    Parameters
    ----------
        name : str
            The name of the resource group.
        mode : str
            Dictates how `WorkQueue` handles exhausted resources. Possible
            values are: `fixed` (task fails), `max` (the maximum allowed
            resource consumption is set by the maximum seen in tasks of
            that category; tasks are automatically adjusted and retried),
            `min_waste` (same as `max`, but allocations prioritize
            minimizing waste), or `max_throughput` (same as `max`, but
            allocations prioritize maximizing throughput.)
        cores : int
            The max number of cores required (`fixed` mode), or the
            first guess for `WorkQueue` to determine the number of
            cores required (all other modes).
        memory : int
            How much memory a task is allowed to use, in megabytes (`fixed`
            mode), or the starting guess for `WorkQueue` to determine how
            much memory a task requires (all other modes).
        disk : int
            How much disk a task is allowed to use, in megabytes (`fixed`
            mode), or the starting guess for `WorkQueue` to determine how
            much disk a task requires (all other modes.)
        runtime : int
            The runtime of the task in seconds.  Lobster will add a grace
            period to this time, and try to adjust the task size such that
            this runtime is achieved.
        tasks_max : int
            How many tasks should be in the queue (running or waiting) at
            the same time.
        tasks_min : int
            The minimum of how many tasks should be in the queue (waiting)
            at the same time.
    """
    _mutable = {
        'tasks_max': (None, [], False),
        'tasks_min': (None, [], False),
        'runtime': ('source.update_runtime', [], True)
    }

    def __init__(self,
                 name,
                 mode='max_throughput',
                 cores=None,
                 memory=None,
                 disk=None,
                 runtime=None,
                 tasks_max=None,
                 tasks_min=None
                 ):
        self.name = name
        self.cores = cores
        self.runtime = runtime
        self.memory = memory
        self.disk = disk
        self.tasks_max = tasks_max
        self.tasks_min = tasks_min

        modes = {
            'fixed': wq.WORK_QUEUE_ALLOCATION_MODE_FIXED,
            'max': wq.WORK_QUEUE_ALLOCATION_MODE_MAX,
            'min_waste': wq.WORK_QUEUE_ALLOCATION_MODE_MIN_WASTE,
            'max_throughput': wq.WORK_QUEUE_ALLOCATION_MODE_MAX_THROUGHPUT
        }

        self.mode = modes[mode]

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def wq(self):
        res = {}
        if self.runtime:
            res['wall_time'] = max(30 * 60, int(1.5 * self.runtime)) * 10 ** 6
        if self.memory:
            res['memory'] = self.memory
        if self.cores:
            res['cores'] = self.cores
        if self.disk:
            res['disk'] = self.disk
        return res


class Workflow(Configurable):

    """
    A specification for processing a dataset.

    Parameters
    ----------
        label : str
            The shorthand name of the workflow.  This is used as a
            reference throughout Lobster.
        dataset : Dataset
            The specification of data to be processed.  Can be any of the
            dataset related classes.
        category : Category
            The category of resource specification this workflow belongs
            to.
        publish_label : str
            The label to be used for the publication database.
        cleanup_input : bool
            Delete input files after processing.
        merge_size : str
            Activates output file merging when set.  Accepts the suffixes
            *k*, *m*, *g* for kilobyte, megabyte, …
        sandbox : Sandbox or list of Sandbox
            The sandbox(es) to use.  Currently can be a
            :class:`~lobster.cmssw.Sandbox`.  When multiple sandboxes are
            used, one sandbox per computing architecture to be run on is
            expected, containing the same release, and an
            :class:`ValueError` will be raised otherwise.
        command : str
            The command to run when executing the workflow.

            The command string may contain `@args`, `@outputfiles`, and
            `@inputfiles`, which will be replaced by unique arguments and
            output as well as input files, respectively.  For running CMSSW
            workflows, it is sufficient to use::

                cmsRun pset.py

            where the file `pset.py` will be automatically added to the
            sandbox and the input source of the parameter set will be
            modified to use the correct input files.  Note that otherwise,
            any used files will have to be included in `extra_inputs`.
        extra_inputs : list
            Additional inputs outside the sandbox needed to process the
            workflow.
        unique_arguments : list
            A list of arguments.  Each element of the dataset is processed
            once for each argument in this list.  The unique argument is
            also passed to the executable.
        outputs : list
            A list of strings which specifies the files produced by the
            workflow. If `outputs=[]`, no output files will be returned.
            If `outputs=None`, outputs will be automatically determined
            for CMSSW workflows.
        output_format : str
            How the output files should be renamed on the storage element.
            This is a new-style format string, allowing for the fields
            `base`, `id`, and `ext`, for the basename of the output file,
            the ID of the task, and the extension of the output file.
        local : bool
            If set to `True`, Lobster will assume this workflow's input is
            present on the output storage element.
        globaltag : str
            Which GlobalTag this workflow uses.  Needed for publication of
            CMSSW workflows, and can be automatically determined for these.
        merge_command : str
            Accepts `cmsRun` (the default), or a custom command.  Tells
            Lobster what command to use for merging. If outputs are
            autodetermined (`outputs=None`), `cmsRun` will be used for EDM
            output and `hadd` will be used otherwise.

            When merging plain ROOT files the following should be used::

                merge_command="hadd @outputfiles @inputfiles"

            See the specification for the `command` parameter about passing
            input and output file values.
        """
    _mutable = {}

    def __init__(self,
                 label,
                 dataset,
                 command,
                 category=Category('default', mode='fixed'),
                 publish_label=None,
                 cleanup_input=False,
                 merge_size=-1,
                 sandbox=None,
                 unique_arguments=None,
                 extra_inputs=None,
                 outputs=None,
                 output_format="{base}_{id}.{ext}",
                 local=False,
                 globaltag=None,
                 merge_command='cmsRun'):
        self.label = label
        if not re.match(r'^[A-Za-z][A-Za-z0-9_]*$', label):
            raise ValueError("Workflow label contains illegal characters: {}".format(label))
        self.category = category
        self.dataset = dataset

        self.publish_label = publish_label if publish_label else label

        self.merge_size = self.__check_merge(merge_size)
        self.cleanup_input = cleanup_input

        self.arguments = shlex.split(command)
        self.command = self.arguments.pop(0)
        self.pset = None
        if self.command == 'cmsRun':
            self.pset = self.arguments.pop(0)
        self.extra_inputs = extra_inputs if extra_inputs else []
        if unique_arguments:
            if any(x is None for x in unique_arguments):
                raise ValueError("Unique arguments should not be None")
            self.unique_arguments = unique_arguments
        else:
            self.unique_arguments = [None]
        self.outputs = outputs
        self.output_format = output_format

        self.dependents = []
        self.parent = None
        if hasattr(dataset, 'parent'):
            self.parent = dataset.parent

        self.globaltag = globaltag
        self.local = local or hasattr(dataset, 'files')
        self.merge_args = shlex.split(merge_command)
        self.merge_command = self.merge_args.pop(0)

        from lobster.cmssw.sandbox import Sandbox
        self.sandbox = sandbox or Sandbox()

    def __repr__(self):
        override = {'category': 'category_' + self.category.name}
        return Configurable.__repr__(self, override)

    def __check_merge(self, size):
        if size <= 0:
            return size

        orig = size
        if isinstance(size, basestring):
            unit = size[-1].lower()
            try:
                size = float(size[:-1])
                if unit == 'k':
                    size *= 1000
                elif unit == 'm':
                    size *= 1e6
                elif unit == 'g':
                    size *= 1e9
                else:
                    size = -1
            except ValueError:
                size = -1
        if size > 0:
            logger.info('merging outputs up to {0} bytes'.format(size))
        else:
            logger.error('merging disabled due to malformed size {0}'.format(orig))
        return size

    def register(self, wflow):
        """Add the workflow `wflow` to the dependents.
        """
        logger.info("marking {0} to be downstream of {1}".format(wflow.label, self.label))
        if len(self.outputs) != 1:
            raise NotImplementedError("dependents for {0} output files not yet supported".format(len(self.outputs)))
        self.dependents.append(wflow)

    def family(self):
        """Returns a flattened hierarchy tree
        """
        yield self
        for d in self.dependents:
            for member in d.family():
                yield member

    def copy_inputs(self, basedirs, overwrite=False):
        """Make a copy of extra input files.

        Includes CMSSW parameter set if specified.  Already present files
        will not be overwritten unless specified.
        """
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

        if self.pset:
            shutil.copy(util.findpath(basedirs, self.pset), os.path.join(self.workdir, os.path.basename(self.pset)))

        if self.extra_inputs is None:
            return []

        def copy_file(fn):
            source = os.path.abspath(util.findpath(basedirs, fn))
            target = os.path.join(self.workdir, os.path.basename(fn))

            if not os.path.exists(target) or overwrite:
                if not os.path.exists(os.path.dirname(target)):
                    os.makedirs(os.path.dirname(target))

                logger.debug("copying '{0}' to '{1}'".format(source, target))
                if os.path.isfile(source):
                    shutil.copy(source, target)
                elif os.path.isdir(source):
                    shutil.copytree(source, target)
                else:
                    logger.error("no such file or directory: {}".format(source))
                    sys.exit(1)

            return target

        files = map(copy_file, self.extra_inputs)
        self.extra_inputs = files

    def autosense(self, releases, basedirs, autoOutputs=False, autoGlobalTag=False):
        """Determine globaltag and output file from a release

        Parameters
        ----------
            releases : list(str)
                a list of releases to go through
            basedirs : list(str)
                a list of directories to search each release in
        """
        for release in releases:
            if not release:
                continue
            reldir = util.findpath(basedirs, release)
            cmd = ['env', '-i', os.path.join(os.path.dirname(__file__), 'data', 'autosense.sh')]
            args = [reldir, os.path.join(self.workdir, os.path.basename(self.pset))] + self.arguments
            try:
                result = json.loads(subprocess.check_output(cmd + args))
                if autoOutputs:
                    self.outputs = result['outputs']
                    self.merge_command = result.get('merge_command', self.merge_command)
                    self.merge_args = result.get('merge_args', self.merge_args)
                if autoGlobalTag:
                    self.globaltag = result.get('globaltag', self.globaltag)
                return
            except:
                e = sys.exc_info()[0:2]
                logger.info(e)
                pass
        else:
            raise RuntimeError("failed to autosense output files and/or global tag")

    def determine_outputs(self, basedirs):
        """Determine output files for CMSSW tasks.
        """
        self.outputs = []

        # To avoid problems loading configs that use the VarParsing module
        sys.argv = [os.path.basename(self.pset)] + self.arguments
        with open(util.findpath(basedirs, self.pset), 'r') as f:
            source = imp.load_source('cms_config_source', self.pset, f)
            process = source.process
            for label, module in process.outputModules.items():
                self.outputs.append(module.fileName.value().replace('file:', ''))
            if 'TFileService' in process.services:
                self.outputs.append(process.services['TFileService'].fileName.value().replace('file:', ''))
                self.merge_command = 'hadd'
                self.merge_args = ['@outputfiles', '@inputfiles']

            logger.info("workflow {0}: adding output file(s) '{1}'".format(self.label, ', '.join(self.outputs)))

    def determine_globaltag(self, basedirs):
        sys.argv = [os.path.basename(self.pset)] + self.arguments
        with open(util.findpath(basedirs, self.pset), 'r') as f:
            source = imp.load_source('cms_config_source', self.pset, f)
            process = source.process
            if hasattr(process, 'GlobalTag') and hasattr(process.GlobalTag.globaltag, 'value'):
                self.globaltag = process.GlobalTag.globaltag.value()

    def validate(self):
        with fs.alternative():
            if not self.dataset.validate():
                msg = "cannot validate configuration for dataset of workflow '{0}'"
                raise AttributeError(msg.format(self.label))
        if fs.exists(self.label) and len(list(fs.ls(self.label))) > 0:
            msg = "stageout directory for '{0}' is not empty"
            raise IOError(msg.format(self.label))
        else:
            # try to create the stageout directory.  if this fails, the
            # user does not have access...
            try:
                fs.makedirs(self.label)
            except Exception:
                msg = "failed to create stageout directory for '{0}'"
                raise IOError(msg.format(self.label))

    def setup(self, workdir, basedirs):
        self.workdir = os.path.join(workdir, self.label)

        if hasattr(self.sandbox, '__iter__'):
            boxes = self.sandbox
        else:
            boxes = [self.sandbox]

        versions = set()
        archs = set()
        self.sandboxes = []
        for box in boxes:
            version, arch, sandbox = box.package(basedirs, workdir)
            versions.add(version)
            if arch in archs:
                raise ValueError("More than one sandbox supplied for the same architecture!")
            archs.add(arch)
            self.sandboxes.append(sandbox)
        if len(versions) > 1:
            raise ValueError("More than one CMSSW version specified!")
        self.version = versions.pop()

        self.copy_inputs(basedirs)
        autoOutputs = False
        autoGlobalTag = False
        if self.outputs == None:
            autoOutputs = True
        if self.globaltag == None:
            autoGlobalTag = True
        if True in (autoOutputs, autoGlobalTag):
            self.autosense([getattr(b, 'release', None) for b in boxes], basedirs, autoOutputs, autoGlobalTag)

        # Working directory for workflow
        # TODO Should we really check if this already exists?  IMO that
        # constitutes an error, since we really should create the workflow!
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

    def handler(self, id_, files, lumis, taskdir, merge=False):
        if merge:
            return MergeTaskHandler(id_, self.label, files, lumis, list(self.get_outputs(id_)), taskdir)
        elif isinstance(self.dataset, MultiProductionDataset) or isinstance(self.dataset, ParentMultiGridpackDataset):
            return MultiProductionTaskHandler(id_, self.label, files, lumis, list(self.get_outputs(id_)), taskdir)
        elif isinstance(self.dataset, ProductionDataset) or isinstance(self.dataset, EmptyDataset):
            return ProductionTaskHandler(id_, self.label, lumis, list(self.get_outputs(id_)), taskdir)
        elif isinstance(self.dataset, MultiGridpackDataset):
            return MultiGridpackTaskHandler(
                id_,
                self.label,
                files,
                lumis,
                list(self.get_outputs(id_)),
                taskdir,
                self.dataset.lumis_per_gridpack)
        else:
            return TaskHandler(id_, self.label, files, lumis, list(self.get_outputs(id_)), taskdir, local=self.local)

    def get_outputs(self, id):
        for fn in self.outputs:
            base, ext = os.path.splitext(fn)
            outfn = self.output_format.format(base=base, ext=ext[1:], id=id)
            yield fn, os.path.join(self.label, outfn)

    def adjust(self, params, env, taskdir, inputs, outputs, merge, reports=None, unique=None):
        cmd = self.command
        args = self.arguments[:]
        pset = os.path.basename(self.pset) if self.pset else self.pset

        env['LOBSTER_CMSSW_VERSION'] = self.version

        for box in self.sandboxes:
            # Remove the hash from the sandbox name
            cleaned = os.path.basename(box).rsplit('-', 1)[0] + '.tar.bz2'
            inputs.append((box, cleaned, True))
        if merge:
            inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'merge_reports.py'), 'merge_reports.py', True))
            inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'task.py'), 'task.py', True))
            inputs.extend((r, "_".join(os.path.normpath(r).split(os.sep)[-3:]), False) for r in reports)

            cmd = self.merge_command
            if cmd == 'cmsRun':
                args = ['outputFile=' + self.outputs[0]]
                pset = os.path.join(os.path.dirname(__file__), 'data', 'merge_cfg.py')
            else:
                args = self.merge_args
                if cmd == 'hadd':
                    args = ['-n', '0', '-f'] + args
                else:
                    inputs.extend((i, os.path.basename(i), True) for i in self.extra_inputs)
                pset = None

            params['prologue'] = None
            params['epilogue'] = ['python', 'merge_reports.py', 'report.json'] \
                + ["_".join(os.path.normpath(r).split(os.sep)[-3:]) for r in reports]
        else:
            inputs.extend((i, os.path.basename(i), True) for i in self.extra_inputs)

            if unique:
                params['arguments_unique'] = shlex.split(unique)
            if pset:
                pset = os.path.join(self.workdir, pset)
            if self.category.runtime:
                # cap task runtime at desired runtime (CMSSW 7.4 and higher
                # only)
                params['task runtime'] = self.category.runtime
            params['cores'] = self.category.cores

        if pset:
            inputs.append((pset, os.path.basename(pset), True))
            outputs.append((os.path.join(taskdir, 'report.xml.gz'), 'report.xml.gz'))

            params['pset'] = os.path.basename(pset)
        elif '@args' not in args and '@inputfiles' not in args and '@outputfiles' not in args:
            params['append inputs to args'] = True

        params['executable'] = cmd
        params['arguments'] = args
        if isinstance(self.dataset, ProductionDataset) and not merge:
            params['mask']['events per lumi'] = self.dataset.events_per_lumi
            params['randomize seeds'] = self.dataset.randomize_seeds
        else:
            params['mask']['events'] = -1
