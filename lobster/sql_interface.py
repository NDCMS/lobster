import os
import sqlite3
from FWCore.PythonUtilities.LumiList import LumiList

class SQLInterface:
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path) #to do: add handling to check for lost jobs from previous execution, set status to failed
        self.db.execute("create table if not exists jobits(job_id, dataset, input_file, run, lumi, status, num_attempts, host, exit_code, run_time, startup_time)")
        self.job_id_counter = 0
        self.db.commit()

    def disconnect(self):
        self.db.close()

    def register_jobits(self, das):
        datasets = [x['dataset'] for x in self.config['tasks']]
        for dataset in datasets:
            dataset_info = das[dataset]
            if self.config.has_key('lumi mask'):
                lumi_mask = LumiList(filename=config['lumi mask'])
                for file in dataset_info.files:
                    dataset_info.lumis[file] = lumi_mask.filterLumis(dataset_info.lumis[file])

            for file in dataset_info.files:
                columns = [(dataset_info.dataset, file, run, lumi) for (run, lumi) in dataset_info.lumis[file]]
                self.db.executemany("insert into jobits(dataset, input_file, run, lumi, status, num_attempts) values (?, ?, ?, ?, 'registered', 0)", columns)

        self.db.commit()

    def pop_jobits(self, size, old_status, new_status):
        self.job_id_counter += 1
        input_files = []
        lumis = []
        for input_file, run, lumi in self.db.execute("select input_file, run, lumi from jobits where status=? limit ?", (old_status, size)):
            input_files.append(input_file)
            lumis.append((run, lumi))
            self.db.execute("update jobits set status=?, job_id=? where run=? and lumi=?", (new_status, self.job_id_counter, run, lumi))

        self.db.commit()

        job_parameters = [set(input_files), LumiList(lumis=lumis).getVLuminosityBlockRange()]

        return job_parameters


