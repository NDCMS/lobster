import os
import random
import sqlite3
from FWCore.PythonUtilities.LumiList import LumiList

# FIXME these are hardcoded in some SQL statements below.  SQLite does not
# seem to have the concept of variables...
INITIALIZED = 0
ASSIGNED = 1
SUCCESSFUL = 2
FAILED = 3
ABORTED = 4
INCOMPLETE = 5

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
            missed_lumis int default 0,
            time_submit int,
            time_transfer_in_start int,
            time_transfer_in_end int,
            time_wrapper_start int,
            time_wrapper_ready int,
            time_file_requested int,
            time_file_opened int,
            time_file_processing int,
            time_wrapper_end int,
            time_transfer_out_start int,
            time_transfer_out_end int,
            time_retrieved int,
            foreign key(dataset) references datasets(id))""")
        self.db.execute("""create table if not exists jobits(
            id integer primary key autoincrement,
            job integer,
            dataset integer,
            input_file text,
            run integer,
            lumi integer,
            status integer default 0,
            attempts int default 0,
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
                where (status<>1 and status<>2) and dataset=?
                limit ?""", (dataset_id, total_size,)):
            if current_size == 0:
                cur = self.db.cursor()
                cur.execute("insert into jobs(dataset, status) values (?, 1)", (dataset_id,))
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
                    LumiList(lumis=lumis)))

                size.pop(0)
                input_files = []
                lumis = []
                current_size = 0

        if len(lumis) > 0:
            jobs.append((
                str(job_id),
                dataset,
                set(input_files),
                LumiList(lumis=lumis)))

        import time
        t = time.time()
        if len(update) > 0:
            self.db.executemany("update jobits set status=1, job=?, attempts=(attempts + 1) where id=?", update)
            self.db.commit()
        else:
            self.db.commit()
            return None
        print time.time() - t

        return jobs

    def reset_jobits(self):
        with self.db as db:
            db.execute("update jobits set status=4 where status=1")
            db.execute("update jobs set status=4 where status=1")

    def update_jobits(self, id, host=None, failed=False, return_code=0, missed_lumis=None, times=None):
        # Don't use [] as a default argument;  it will get set by the first
        # call
        if not missed_lumis:
            missed_lumis = []
        if not times:
            times = [None] * 12

        missed = len(missed_lumis)

        if failed:
            status = FAILED
        elif missed > 0:
            status = INCOMPLETE
        else:
            status = SUCCESSFUL

        with self.db as db:
            db.execute("""update jobits set
                status=?
                where job=?""",
                (status, int(id)))
            db.execute("""update jobs set
                status=?,
                host=?,
                exit_code=?,
                time_submit=?,
                time_transfer_in_start=?,
                time_transfer_in_end=?,
                time_wrapper_start=?,
                time_wrapper_ready=?,
                time_file_requested=?,
                time_file_opened=?,
                time_file_processing=?,
                time_wrapper_end=?,
                time_transfer_out_start=?,
                time_transfer_out_end=?,
                time_retrieved=?,
                missed_lumis=?
                where id=?""",
                [status, host, return_code] + times + [missed, int(id)])
            if status != SUCCESSFUL:
                for run, lumi in missed_lumis:
                    db.execute("update jobits set status=? where job=? and run=? and lumi=?",
                            (FAILED, int(id), run, lumi))

    def unfinished_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status!=?", (SUCCESSFUL,))
        return cur.fetchone()[0]

    def running_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status==?", (ASSIGNED,))
        return cur.fetchone()[0]
