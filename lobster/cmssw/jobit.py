import os
import sqlite3
from FWCore.PythonUtilities.LumiList import LumiList

class SQLInterface:
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path)
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

    def pop_jobits(self, size):
        self.job_id_counter += 1
        id = str(self.job_id_counter)
        input_files = []
        lumis = []
        dset = None
        for dataset, input_file, run, lumi in self.db.execute("""
                select dataset, input_file, run, lumi
                from jobits
                where status='registered' or status='failed'
                group by dataset, input_file
                limit ?""", (size,)):
            if dset == None:
                dset = dataset
            elif dset != dataset:
                break

            input_files.append(input_file)
            lumis.append((run, lumi))
            self.db.execute("update jobits set status='in progress', job_id=? where run=? and lumi=?",
                (id, run, lumi))

        self.db.commit()

        job_parameters = [id, dset, set(input_files), LumiList(lumis=lumis).getVLuminosityBlockRange()]

        return job_parameters

    def reset_jobits(self):
        with self.db as db:
            db.execute("""update jobits set status='failed' where status='in progress'""")

    def update_jobits(self, id, failed=False):
        with self.db as db:
            db.execute("""update jobits set status=? where job_id=?""",
                    ('failed' if failed else 'successful', id))

    def unfinished_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status!='successful'")
        return cur.fetchone()[0]
