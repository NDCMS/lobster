import os
import sqlite3
from FWCore.PythonUtilities.LumiList import LumiList

class SQLInterface:
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path)
        self.db.execute("create table if not exists jobits(job_id integer, dataset, input_file, run, lumi, status, num_attempts, host, exit_code, run_time, startup_time)")
        self.db.commit()
        self.job_id_counter = 0

        try:
            cur = self.db.execute("select max(job_id) from jobits")
            count = int(cur.fetchone()[0])
            if count:
                print "Restarting with job counter", count
                self.job_id_counter = count
        except:
            pass

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
                group by dataset, input_file, lumi
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
        return [id, dset, set(input_files), LumiList(lumis=lumis).getVLuminosityBlockRange()]

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
