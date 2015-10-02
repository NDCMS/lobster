import os

class Workflow(object):
    def __init__(self, workdir, config):
        self.config = config
        self.label = config['label']
        self.workdir = os.path.join(workdir, self.label)

        self.cmd = config.get('cmd', 'cmsRun')
        self.extra_inputs = config.get('extra inputs', [])
        self.args = config.get('parameters', [])
        self._outputs = config.get('outputs')
        self.outputformat = config.get("output format", "{base}_{id}.{ext}")

        self.events_per_task = config.get('events per job', -1)
        self.pset = config.get('cmssw config')
        self.local = config.get('local', 'files' in config)
        self.edm_output = config.get('edm output', True)

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
                args = ['output=' + self.outputs[0]]
                pset = os.path.join(os.path.dirname(__file__), 'data', 'merge_cfg.py')
                params['append inputs to args'] = True
            else:
                cmd = 'hadd'
                args = ['-f', self.outputs[0]]
                pset = None

            params['prologue'] = None
            params['epilogue'] = ['python', 'merge_reports.py', 'report.json'] \
                    + ["_".join(os.path.normpath(r).split(os.sep)[-3:]) for r in reports]
        else:
            inputs.extend((i, os.path.basename(i), True) for i in self.extra_inputs)

            if unique:
                args.append(unique)

            if pset:
                pset = os.path.join(self.workdir, pset)

        if pset:
            inputs.append((pset, os.path.basename(pset), True))
            outputs.append((os.path.join(taskdir, 'report.xml.gz'), 'report.xml.gz'))

            params['pset'] = os.path.basename(pset)

        params['executable'] = cmd
        params['arguments'] = args
        params['mask']['events'] = self.events_per_task
