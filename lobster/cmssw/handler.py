from lobster.cmssw import jobit

from FWCore.PythonUtilities.LumiList import LumiList

class TaskHandler(object):
    """
    Handles mapping of lumi sections to files etc.
    """

    def __init__(
            self, id, dataset, files, lumis, outputs, jobdir,
            cmssw_job=True, empty_source=False, merge=False, local=False):
        self._id = id
        self._dataset = dataset
        self._files = [(id, file) for id, file in files]
        self._file_based = any([run < 0 or lumi < 0 for (id, file, run, lumi) in lumis])
        self._jobits = lumis
        self._jobdir = jobdir
        self._outputs = outputs
        self._merge = merge
        self._cmssw_job = cmssw_job
        self._empty_source = empty_source
        self._local = local

    @property
    def cmssw_job(self):
        return self._cmssw_job

    @property
    def dataset(self):
        return self._dataset

    @property
    def outputs(self):
        return self._outputs

    @property
    def id(self):
        return self._id

    @property
    def jobdir(self):
        return self._jobdir

    @property
    def input_files(self):
        return list(set([filename for (id, filename) in self._files if filename]))

    @property
    def jobit_source(self):
        return 'jobs' if self._merge else 'jobits_' + self._dataset

    @property
    def merge(self):
        return self._merge

    def get_jobit_info(self, failed, files_info, files_skipped, events_written):
        events_read = 0
        file_update = []
        jobit_update = []

        jobits_processed = len(self._jobits)

        for (id, file) in self._files:
            file_jobits = [tpl for tpl in self._jobits if tpl[1] == id]

            skipped = False
            read = 0
            if self._cmssw_job:
                if not self._empty_source:
                    skipped = file in files_skipped or file not in files_info
                    read = 0 if failed or skipped else files_info[file][0]

            events_read += read

            if failed:
                jobits_processed = 0
            else:
                if skipped:
                    for (lumi_id, lumi_file, r, l) in file_jobits:
                        jobit_update.append((jobit.FAILED, lumi_id))
                        jobits_processed -= 1
                elif not self._file_based:
                    file_lumis = set(map(tuple, files_info[file][1]))
                    for (lumi_id, lumi_file, r, l) in file_jobits:
                        if (r, l) not in file_lumis:
                            jobit_update.append((jobit.FAILED, lumi_id))
                            jobits_processed -= 1

            file_update.append((read, 1 if skipped else 0, id))

        if failed:
            events_written = 0
            status = jobit.FAILED
        else:
            status = jobit.SUCCESSFUL

        if self._merge:
            file_update = []
            # FIXME not correct
            jobits_missed = 0

        return jobits_processed, events_read, events_written, status, \
                file_update, jobit_update

    def adjust(self, parameters, inputs, outputs, se):
        local = self._local or self._merge
        if local and se.transfer_inputs():
            inputs += [(se.local(f), os.path.basename(f), False) for id, f in self._files if f]
        if se.transfer_outputs():
            outputs += [(se.local(rf), os.path.basename(lf)) for lf, rf in self._outputs]

        parameters['mask']['files'] = self.input_files
        parameters['output files'] = self._outputs
        if not self._file_based and not self._merge:
            ls = LumiList(lumis=set([(run, lumi) for (id, file, run, lumi) in self._jobits]))
            parameters['mask']['lumis'] = ls.getCompactList()
