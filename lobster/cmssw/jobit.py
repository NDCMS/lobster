import os
import random
import sqlite3
from FWCore.PythonUtilities.LumiList import LumiList

INITIALIZED = 0
ASSIGNED = 1
SUCCESSFUL = 2
FAILED = 3
ABORTED = 4

class SQLInterface:
    def __init__(self, config):
        self.config = config
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path)

        # Use four databases: one for jobits, jobs, hosts, datasets each
        self.db.execute("""create table if not exists datasets(
            id integer primary key autoincrement,
            label text,
            path text,
            jobits integer,
            jobits_done int default 0)""")
        self.db.execute("""create table if not exists jobs(
            id integer primary key autoincrement,
            host text,
            dataset integer,
            status int default 0,
            exit_code integer,
            attempts int default 0,
            startup_time real,
            processing_time real,
            run_time real,
            foreign key(dataset) references datasets(id))""")
        self.db.execute("""create table if not exists jobits(
            id integer primary key autoincrement,
            job integer,
            dataset integer,
            input_file text,
            run integer,
            lumi integer,
            status integer default 0,
            foreign key(job) references jobs(id),
            foreign key(dataset) references datasets(id))""")
        self.db.execute("create index if not exists file_index on jobits(dataset, input_file asc)")
        self.db.execute("create index if not exists event_index on jobits(dataset, run, lumi)")
        self.db.commit()

        # self.db.execute("create table if not exists jobits(job_id integer, ds_label, input_file, run, lumi, status, num_attempts, host, exit_code, run_time, startup_time)")

        try:
            cur = self.db.execute("select max(id) from jobs")
            count = int(cur.fetchone()[0])
            if count:
                print "Restarting with job counter", count
        except:
            pass

    def disconnect(self):
        self.db.close()

    def register_jobits(self, dataset_interface):
        labels = [x['dataset label'] for x in self.config['tasks']]
        for label in labels:
            dataset_info = dataset_interface[label]

            if self.config.has_key('lumi mask'):
                lumi_mask = LumiList(filename=config['lumi mask'])
                for file in dataset_info.files:
                    dataset_info.lumis[file] = lumi_mask.filterLumis(dataset_info.lumis[file])

            cur = self.db.cursor()
            cur.execute("insert into datasets(label) values (?)", (label,))
            id = cur.lastrowid

            for file in dataset_info.files:
                columns = [(id, file, run, lumi) for (run, lumi) in dataset_info.lumis[file]]
                self.db.executemany("insert into jobits(dataset, input_file, run, lumi) values (?, ?, ?, ?)", columns)

        self.db.commit()

    def pop_jobits(self, size=None):
        if not size:
            size = [5]

        current_size = 0
        total_size = sum(size)

        input_files = []
        lumis = []

        jobs = []
        update = []

        rows = [xs for xs in self.db.execute("select label, id from datasets")]
        dataset, dataset_id = random.choice(rows)

        for id, input_file, run, lumi in self.db.execute("""
                select id, input_file, run, lumi
                from jobits
                where (status=0 or status=2 or status=3) and dataset=?
                limit ?""", (dataset_id, total_size,)):
            if current_size == 0:
                cur = self.db.cursor()
                cur.execute("insert into jobs(dataset) values (?)", (dataset,))
                job_id = cur.lastrowid

            input_files.append(input_file)
            if lumi > 0:
                lumis.append((run, lumi))
            update.append((job_id, id))

            current_size += 1

            if current_size == size[0]:
                jobs.append((
                    str(job_id),
                    dataset,
                    set(input_files),
                    LumiList(lumis=lumis).getVLuminosityBlockRange()))

                size.pop(0)
                input_files = []
                current_size = 0

        import time
        t = time.time()
        if len(update) > 0:
            self.db.executemany("update jobits set status=1, job=? where id=?", update)
            self.db.commit()
        else:
            self.db.commit()
            return None
        print time.time() - t

        return jobs

    def reset_jobits(self):
        with self.db as db:
            db.execute("update jobits set status=? where status=?", (ABORTED, ASSIGNED))
            db.execute("update jobs set status=? where status=?", (ABORTED, ASSIGNED))

    def update_jobits(self, id, failed=False):
        with self.db as db:
            db.execute("update jobits set status=? where job=?",
                       (FAILED if failed else SUCCESSFUL, int(id)))
            db.execute("update jobs set status=? where id=?",
                       (FAILED if failed else SUCCESSFUL, int(id)))

    def unfinished_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status!=?", (SUCCESSFUL,))
        return cur.fetchone()[0]

    def running_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status==?", (ASSIGNED,))
        return cur.fetchone()[0]
