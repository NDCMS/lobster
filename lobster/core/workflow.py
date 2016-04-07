# -*- coding: utf8 -*-
import imp
import logging
import os
import re
import shutil
import sys

from lobster import fs, util
from lobster.cmssw import sandbox
from lobster.core.dataset import ProductionDataset
from lobster.core.task import *
from lobster.util import Configurable

logger = logging.getLogger('lobster.workflow')

class Category(Configurable):
    """
    Resource specification for a group of
    :class:`~lobster.core.workflow.Workflow`s.

    This information will be passed on to `WorkQueue`, which will forcibly
    terminate tasks of :class:`.Workflow` in the group that exceed the
    specified resources.

    Parameters
    ----------
        name : str
            The name of the resource group.
        cores : int
            The number of cores a task of the group should use.
        runtime : int
            The runtime of the task in seconds.  Lobster will add a grace
            period to this time, and try to adjust the task size such that
            this runtime is achieved.
        memory : int
            How much memory a task is allowed to use, in megabytes.
        disk : int
            How much disk a task is allowed to use, in megabytes.
        tasks : int
            How many tasks should be in the queue (running or waiting) at
            the same time.
    """
    _mutable = {
            'tasks': (None, None, tuple())
    }

    def __init__(self,
            name,
            cores=1,
            runtime=None,
            memory=None,
            disk=None,
            tasks=None
            ):
        self.name = name
        self.cores = cores
        self.runtime = runtime
        self.memory = memory
        self.disk = disk
        self.tasks = tasks

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def wq(self):
        res = {}
        if self.runtime:
            res['wall_time'] = max(30 * 60, int(1.5 * self.runtime)) * 10**6
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
        merge_cleanup : bool
            Delete merged output files.
        merge_size : str
            Activates output file merging when set.  Accepts the suffixes
            *k*, *m*, *g* for kilobyte, megabyte, …
        sandbox : str
            Path to a sandbox to re-use.  This sandbox will be copied over
            to the current working directory.
        sandbox_release : str
            The path to the CMSSW release to be used as a sandbox.
            Defaults to the environment variable `LOCALRT`.
        sandbox_blacklist : list
            A specification of paths to not pack into the sandbox.
        command : str
            Which executable to run (for non-CMSSW workflows)
        extra_inputs : list
            Additional inputs outside the sandbox needed to process the
            workflow.
        arguments : list
            Arguments to pass to the executable.
        unique_arguments : list
            A list of arguments.  Each element of the dataset is processed
            once for each argument in this list.  The unique argument is
            also passed to the executable.

            TODO: should really be in the dataset specification
        outputs : list
            A list of strings which specifies the files produced by the
            workflow.  Will be automatically determined for CMSSW
            workflows.
        output_format : str
            How the output files should be renamed on the storage element.
            This is a new-style format string, allowing for the fields
            `base`, `id`, and `ext`, for the basename of the output file,
            the ID of the task, and the extension of the output file.
        local : bool
            If set to `True`, Lobster will assume this workflow's input is
            present on the output storage element.
        pset : str
            The CMSSW configuration to use, if any.
        globaltag : str
            Which GlobalTag this workflow uses.  Needed for publication of
            CMSSW workflows, and can be automatically determined for these.

            TODO: check that the globaltag is determined independently of
            the outputs.
        edm_output : bool
            Autodetermined when outputs are determined automatically.
            Tells Lobster if the output of this workflow is in EDM format.
    """
    _mutable = {}
    def __init__(self,
            label,
            dataset,
            category=None,
            publish_label=None,
            merge_cleanup=True,
            merge_size=-1,
            sandbox=None,
            sandbox_release=None,
            sandbox_blacklist=None,
            command='cmsRun',
            extra_inputs=None,
            arguments=None,
            unique_arguments=None,
            outputs=None,
            output_format="{base}_{id}.{ext}",
            local=False,
            pset=None,
            globaltag=None,
            edm_output=True):
        self.label = label
        if not re.match(r'^[A-Za-z][A-Za-z0-9_]*$', label):
            raise ValueError("Workflow label contains illegal characters: {}".format(label))
        self.category = category if category else label
        self.dataset = dataset

        self.publish_label = publish_label if publish_label else label

        self.merge_size = self.__check_merge(merge_size)
        self.merge_cleanup = merge_cleanup

        self.command = command
        self.extra_inputs = extra_inputs if extra_inputs else []
        self.arguments = arguments if arguments else []
        self.unique_arguments = unique_arguments if unique_arguments else [None]
        self.outputs = outputs
        self.output_format = output_format

        self.dependents = []
        self.parent = None
        if hasattr(dataset, 'parent'):
            self.parent = dataset.parent

        self.pset = pset
        self.globaltag = globaltag
        self.local = local or hasattr(dataset, 'files')
        self.edm_output = edm_output

        self.sandbox = sandbox
        if sandbox_release is not None:
            self.sandbox_release = sandbox_release
        else:
            try:
                self.sandbox_release = os.environ['LOCALRT']
            except:
                raise AttributeError("Need to be either in a `cmsenv` or specify a sandbox release!")
        self.sandbox_blacklist = sandbox_blacklist

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
                    raise NotImplementedError

            return target

        files = map(copy_file, self.extra_inputs)
        self.extra_inputs = files

    def determine_outputs(self, basedirs):
        """Determine output files for CMSSW tasks.
        """
        self.outputs = []
        # Save determined outputs to the configuration in the
        # working directory.
        update_config = True

        # To avoid problems loading configs that use the VarParsing module
        sys.argv = [os.path.basename(self.pset)] + self.arguments
        with open(util.findpath(basedirs, self.pset), 'r') as f:
            source = imp.load_source('cms_config_source', self.pset, f)
            process = source.process
            if hasattr(process, 'GlobalTag') and hasattr(process.GlobalTag.globaltag, 'value'):
                self.global_tag = process.GlobalTag.globaltag.value()
            for label, module in process.outputModules.items():
                self.outputs.append(module.fileName.value().replace('file:', ''))
            if 'TFileService' in process.services:
                self.outputs.append(process.services['TFileService'].fileName.value().replace('file:', ''))
                self.edm_output = False

            logger.info("workflow {0}: adding output file(s) '{1}'".format(self.label, ', '.join(self.outputs)))

    def setup(self, workdir, basedirs):
        self.workdir = os.path.join(workdir, self.label)

        if self.sandbox:
            self.version, self.sandbox = sandbox.recycle(self.sandbox, workdir)
        else:
            self.version, self.sandbox = sandbox.package(
                    util.findpath(basedirs, self.sandbox_release),
                    workdir,
                    self.sandbox_blacklist)

        self.copy_inputs(basedirs)
        if self.pset and not self.outputs:
            self.determine_outputs(basedirs)

        # Working directory for workflow
        # TODO Should we really check if this already exists?  IMO that
        # constitutes an error, since we really should create the workflow!
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)
        # Create the stageout directory
        if not fs.exists(self.label):
            fs.makedirs(self.label)
        else:
            if len(list(fs.ls(self.label))) > 0:
                msg = 'stageout directory is not empty: {0}'
                raise IOError(msg.format(fs.__getattr__('lfn2pfn')(self.label)))

    def handler(self, id_, files, lumis, taskdir, merge=False):
        if merge:
            return MergeTaskHandler(id_, self.label, files, lumis, list(self.get_outputs(id_)), taskdir)
        elif isinstance(self.dataset, ProductionDataset):
            return ProductionTaskHandler(id_, self.label, lumis, list(self.get_outputs(id_)), taskdir)
        else:
            return TaskHandler(id_, self.label, files, lumis, list(self.get_outputs(id_)), taskdir, local=self.local)

    def get_outputs(self, id):
        for fn in self.outputs:
            base, ext = os.path.splitext(fn)
            outfn = self.output_format.format(base=base, ext=ext[1:], id=id)
            yield fn, os.path.join(self.label, outfn)

    def adjust(self, params, taskdir, inputs, outputs, merge, reports=None, unique=None):
        cmd = self.command
        args = self.arguments
        pset = os.path.basename(self.pset)

        inputs.append((self.sandbox, 'sandbox.tar.bz2', True))
        if merge:
            inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'merge_reports.py'), 'merge_reports.py', True))
            inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'task.py'), 'task.py', True))
            inputs.extend((r, "_".join(os.path.normpath(r).split(os.sep)[-3:]), False) for r in reports)

            if self.edm_output:
                args = ['output=' + self.outputs[0]]
                pset = os.path.join(os.path.dirname(__file__), 'data', 'merge_cfg.py')
            else:
                cmd = 'hadd'
                args = ['-n', '0', '-f', self.outputs[0]]
                pset = None
                params['append inputs to args'] = True

            params['prologue'] = None
            params['epilogue'] = ['python', 'merge_reports.py', 'report.json'] \
                    + ["_".join(os.path.normpath(r).split(os.sep)[-3:]) for r in reports]
        else:
            inputs.extend((i, os.path.basename(i), True) for i in self.extra_inputs)

            if unique:
                args.append(unique)
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

        params['executable'] = cmd
        params['arguments'] = args
        if isinstance(self.dataset, ProductionDataset) and not merge:
            params['mask']['events'] = self.dataset.events_per_task
            params['mask']['events per lumi'] = self.dataset.events_per_lumi
            params['randomize seeds'] = self.dataset.randomize_seeds
        else:
            params['mask']['events'] = -1
