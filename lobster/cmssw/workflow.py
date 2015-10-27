import imp
import logging
import os
import shutil
import sys

from lobster import fs, util

logger = logging.getLogger('lobster.cmssw.workflow')

class Workflow(object):
    def __init__(self, workdir, config, basedirs):
        self.config = config
        self.label = config['label']
        self.workdir = os.path.join(workdir, self.label)
        self.runtime = config.get('task runtime')

        self.cmd = config.get('cmd', 'cmsRun')
        self.extra_inputs = config.get('extra inputs', [])
        self.args = config.get('parameters', [])
        self._outputs = config.get('outputs')
        self.outputformat = config.get("output format", "{base}_{id}.{ext}")

        self.events_per_task = config.get('events per job', -1)
        self.pset = config.get('cmssw config')
        self.local = config.get('local', 'files' in config)
        self.edm_output = config.get('edm output', True)

        self.copy_inputs(basedirs)
        if self.pset and not self._outputs:
            self.determine_outputs(basedirs)

    def copy_inputs(self, basedirs, overwrite=False):
        """Make a copy of extra input files.

        Includes CMSSW parameter set if specified.  Already present files
        will not be overwritten unless specified.
        """
        if not os.path.exists(self.workdir):
            os.makedirs(self.workdir)

        if self.pset:
            shutil.copy(util.findpath(basedirs, self.pset), os.path.join(self.workdir, os.path.basename(self.pset)))

        if 'extra inputs' not in self.config:
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

        files = map(copy_file, self.config['extra inputs'])
        self.config['extra inputs'] = files
        self.extra_inputs = files

    def determine_outputs(self, basedirs):
        """Determine output files for CMSSW jobs.
        """
        self._outputs = []
        # Save determined outputs to the configuration in the
        # working directory.
        update_config = True

        # To avoid problems loading configs that use the VarParsing module
        sys.argv = [os.path.basename(self.pset)] + self.args
        with open(util.findpath(basedirs, self.pset), 'r') as f:
            source = imp.load_source('cms_config_source', self.pset, f)
            process = source.process
            if hasattr(process, 'GlobalTag') and hasattr(process.GlobalTag.globaltag, 'value'):
                self.config['global tag'] = process.GlobalTag.globaltag.value()
            for label, module in process.outputModules.items():
                self._outputs.append(module.fileName.value())
            if 'TFileService' in process.services:
                self._outputs.append(process.services['TFileService'].fileName.value())
                self.edm_output = False

            self.config['edm output'] = self.edm_output
            self.config['outputs'] = self._outputs

            logger.info("workflow {0}: adding output file(s) '{1}'".format(label, ', '.join(self._outputs)))

    def create(self):
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

    def outputs(self, id):
        for fn in self._outputs:
            base, ext = os.path.splitext(fn)
            outfn = self.outputformat.format(base=base, ext=ext[1:], id=id)
            yield fn, os.path.join(self.label, outfn)

    def adjust(self, params, taskdir, inputs, outputs, merge, reports=None, unique=None):
        cmd = self.cmd
        args = self.args
        pset = self.pset

        if merge:
            inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'merge_reports.py'), 'merge_reports.py', True))
            inputs.append((os.path.join(os.path.dirname(__file__), 'data', 'job.py'), 'job.py', True))
            inputs.extend((r, "_".join(os.path.normpath(r).split(os.sep)[-3:]), False) for r in reports)

            if self.edm_output:
                args = ['output=' + self._outputs[0]]
                pset = os.path.join(os.path.dirname(__file__), 'data', 'merge_cfg.py')
            else:
                cmd = 'hadd'
                args = ['-n', '0', '-f', self._outputs[0]]
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
            if self.runtime:
                # cap task runtime at desired runtime + 10 minutes grace
                # period (CMSSW 7.4 and higher only)
                params['task runtime'] = self.runtime + 10 * 60

        if pset:
            inputs.append((pset, os.path.basename(pset), True))
            outputs.append((os.path.join(taskdir, 'report.xml.gz'), 'report.xml.gz'))

            params['pset'] = os.path.basename(pset)

        params['executable'] = cmd
        params['arguments'] = args
        params['mask']['events'] = self.events_per_task
