import os
import sqlite3
import lobster.jobit
from FWCore.PythonUtilities.LumiList import LumiList

class SQLInterface(lobster.jobit.SQLInterface):
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path)
        self.db.execute("create table if not exists jobits(job_id integer, dataset, input_file, run, lumi, status, num_attempts, host, exit_code, run_time, startup_time)")
        self.db.execute("create index if not exists event_index on jobits(dataset, run, lumi)")
        self.db.execute("create index if not exists file_index on jobits(dataset, input_file asc)")
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
                self.db.executemany("insert into jobits(dataset, input_file, run, lumi, status, num_attempts) values (?, ?, ?, ?, 'i', 0)", columns)

        self.db.commit()

    def pop_jobits(self, size=5):
        self.job_id_counter += 1
        id = str(self.job_id_counter)

        input_files = []
        lumis = []
        update = []

        last_dataset = None

        for dataset, input_file, run, lumi in self.db.execute("""
                select dataset, input_file, run, lumi
                from jobits
                where (status='i' or status='f')
                order by dataset, input_file
                limit ?""", (size,)):
            if last_dataset is None:
                last_dataset = dataset
            elif last_dataset != dataset:
                break

            input_files.append(input_file)
            lumis.append((run, lumi))
            update.append((int(id), dataset, run, lumi))

        self.db.executemany("update jobits set status='r', job_id=? where dataset=? and run=? and lumi=?", update)
        self.db.commit()
        return [id, dataset, set(input_files), LumiList(lumis=lumis).getVLuminosityBlockRange()]

