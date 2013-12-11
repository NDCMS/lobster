import os
import sqlite3
from FWCore.PythonUtilities.LumiList import LumiList

class SQLInterface:
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path)
        self.db.execute("create table if not exists jobits(job_id integer, ds_label, input_file, run, lumi, status, num_attempts, host, exit_code, run_time, startup_time)")
        self.db.execute("create index if not exists event_index on jobits(ds_label, run, lumi)")
        self.db.execute("create index if not exists file_index on jobits(ds_label, input_file asc)")
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

    def register_jobits(self, ds_interface):
        ds_labels = [x['dataset label'] for x in self.config['tasks']]
        for ds_label in ds_labels:
            dataset_info = ds_interface[ds_label]
            if self.config.has_key('lumi mask'):
                lumi_mask = LumiList(filename=config['lumi mask'])
                for file in dataset_info.files:
                    dataset_info.lumis[file] = lumi_mask.filterLumis(dataset_info.lumis[file])

            for file in dataset_info.files:
                columns = [(dataset_info.label, file, run, lumi) for (run, lumi) in dataset_info.lumis[file]]
                self.db.executemany("insert into jobits(ds_label, input_file, run, lumi, status, num_attempts) values (?, ?, ?, ?, 'i', 0)", columns)

        self.db.commit()

    def pop_jobits(self, size=5):
        self.job_id_counter += 1
        id = str(self.job_id_counter)

        input_files = []
        lumis = []
        update = []

        last_dataset = None

        for ds_label, input_file, run, lumi in self.db.execute("""
                select ds_label, input_file, run, lumi
                from jobits
                where (status='i' or status='f')
                order by ds_label, input_file
                limit ?""", (size,)):
            if last_dataset is None:
                last_dataset = ds_label
            elif last_dataset != ds_label:
                break

            input_files.append(input_file)
            if lumi > 0:
                lumis.append((run, lumi))
            update.append((int(id), ds_label, run, lumi, input_file))

        #input file is needed for dataset lists, when we don't have run/lumi info to differentiate between jobits.  Is this too slow?  Alternatively, we could add a jobit id.
        self.db.executemany("update jobits set status='r', job_id=? where ds_label=? and run=? and lumi=? and input_file=?", update)
        self.db.commit()
        return [id, ds_label, set(input_files), LumiList(lumis=lumis).getVLuminosityBlockRange()]

    def reset_jobits(self):
        with self.db as db:
            db.execute("update jobits set status='f' where status='r'")

    def update_jobits(self, id, failed=False):
        with self.db as db:
            db.execute("update jobits set status=? where job_id=?",
                       ('f' if failed else 's', int(id)))

    def unfinished_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status!='s'")
        return cur.fetchone()[0]

    def running_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status=='r'")
        return cur.fetchone()[0]
