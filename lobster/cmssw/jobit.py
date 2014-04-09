import os
import random
import sqlite3
import time
from FWCore.PythonUtilities.LumiList import LumiList
import uuid

# FIXME these are hardcoded in some SQL statements below.  SQLite does not
# seem to have the concept of variables...
INITIALIZED = 0
ASSIGNED = 1
SUCCESSFUL = 2
FAILED = 3
ABORTED = 4
INCOMPLETE = 5
PUBLISHED = 6

class SQLInterface:
    def __init__(self, config):
        self.uuid = str(uuid.uuid4()).replace('-', '')
        self.config = config
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path)

        # Use four databases: one for jobits, jobs, hosts, datasets each
        self.db.execute("""create table if not exists datasets(
            id integer primary key autoincrement,
            dataset text,
            label text,
            path text,
            release text,
            global_tag text,
            dbs_url text,
            publish_label text,
            pset_hash text default null,
            cfg text,
            uuid text,
            jobits integer,
            jobits_running int default 0,
            jobits_done int default 0,
            total_events int default 0,
            processed_events int default 0)""")
        self.db.execute("""create table if not exists jobs(
            id integer primary key autoincrement,
            host text,
            dataset int,
            published_file_block text,
            status int default 0,
            exit_code int,
            retries int default 0,
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
            time_on_worker int,
            time_total_on_worker int,
            bytes_received int,
            bytes_sent int,
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
        self.db.execute("create index if not exists dataset_index on jobits(dataset)")
        self.db.execute("create index if not exists event_index on jobits(dataset, run, lumi)")
        self.db.execute("create index if not exists file_index on jobits(input_file)")
        self.db.execute("create index if not exists dfile_index on jobits(dataset, input_file asc)")
        self.db.execute("create index if not exists nfile_index on jobits(attempts, input_file)")
        self.db.execute("create index if not exists job_index on jobits(job, run, lumi)")
        self.db.commit()

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
        dbs_url = self.config.get('dbs url')
        for cfg in self.config['tasks']:
            label = cfg['dataset label']
            print "Registering {0}...".format(label)
            dataset_info = dataset_interface[label]

            if cfg.has_key('lumi mask'):
                lumi_mask = LumiList(filename=cfg['lumi mask'])
                for file in dataset_info.files:
                    dataset_info.lumis[file] = lumi_mask.filterLumis(dataset_info.lumis[file])

            cur = self.db.cursor()
            cur.execute("""insert into datasets
                           (dataset,
                           label,
                           path,
                           release,
                           global_tag,
                           dbs_url,
                           publish_label,
                           cfg,
                           uuid,
                           total_events)
                           values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                               cfg['dataset'],
                               label,
                               os.path.join(self.config['stageout location'], label),
                               os.path.basename(os.environ['LOCALRT']),
                               cfg.get('global tag'),
                               dbs_url,
                               cfg.get('publish label', cfg['dataset label']).replace('-', '_'), #TODO: more lexical checks #TODO: publish label check
                               cfg['cmssw config'],
                               self.uuid,
                               dataset_info.total_events))
            id = cur.lastrowid

            lumis = 0
            for file in dataset_info.files:
                columns = [(id, file, run, lumi) for (run, lumi) in dataset_info.lumis[file]]
                lumis += len(columns)
                self.db.executemany("insert into jobits(dataset, input_file, run, lumi) values (?, ?, ?, ?)", columns)
            self.db.execute("update datasets set jobits=? where id=?", (lumis, id))
        self.db.commit()

    def pop_jobits(self, size=None, bijective=False):
        if not size:
            size = [5]

        t = time.time()

        current_size = 0
        total_size = sum(size)

        input_files = []
        lumis = []
        total_lumis = 0

        jobs = []
        update = []

        rows = [xs for xs in self.db.execute("""
            select label, id, jobits - jobits_done - jobits_running, jobits
            from datasets
            where jobits_done + jobits_running < jobits""")]
        if len(rows) == 0:
            return None
        dataset, dataset_id, remaining, total = random.choice(rows)

        if bijective:
            size = []
            rows = []
            for file in self.db.execute("""
                    select distinct input_file
                    from jobits
                    where dataset=? and (status<>1 and status<>2)
                    limit ?""", (dataset_id, total_size,)):
                rows.extend(self.db.execute("""
                    select id, input_file, run, lumi
                    from jobits
                    where input_file=? and (status<>1 and status<>2)""", file))
                if len(size) > 0:
                    size.append(len(rows)-size[-1])
                else:
                    size.append(len(rows))
        else:
            rows = self.db.execute("""
                select id, input_file, run, lumi
                from jobits
                where dataset=? and (status<>1 and status<>2)
                order by attempts, input_file
                limit ?""", (dataset_id, total_size,))

        for id, input_file, run, lumi in rows:
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

                total_lumis += len(lumis)

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

            total_lumis += len(lumis)

        if total_lumis > 0:
            self.db.execute(
                    "update datasets set jobits_running=(jobits_running + ?) where id=?",
                    (total_lumis, dataset))

        if len(update) > 0:
            self.db.executemany("update jobits set status=1, job=?, attempts=(attempts + 1) where id=?", update)

        self.db.commit()

        with open(os.path.join(self.config["workdir"], 'debug_sql_times'), 'a') as f:
            delta = time.time() - t
            size = len(jobs)
            ratio = delta / float(size) if size != 0 else 0
            f.write("CREA {0} {1} {2}\n".format(size, delta, ratio))

        return jobs if len(update) > 0 else None

    def reset_jobits(self):
        with self.db as db:
            ids = [id for (id,) in db.execute("select id from jobs where status=1")]
            db.execute("update datasets set jobits_running=0")
            db.execute("update jobits set status=4 where status=1")
            db.execute("update jobs set status=4 where status=1")
        return ids

    def update_jobits(self, jobs):
        up_jobits = []
        up_jobs = []
        up_missed = []

        dsets = {}
        for job in jobs:
            (id, dset, host, failed, return_code, retries, processed_lumis, missed_lumis, times, data, processed_events) = job

            id = int(id)

            missed = len(missed_lumis)
            processed = len(processed_lumis)

            try:
                dsets[dset][0] += missed + processed
                dsets[dset][1] += processed
                dsets[dset][2] += processed_events
            except KeyError:
                dsets[dset] = [missed + processed, processed, processed_events]

            if failed:
                status = FAILED
            elif missed > 0:
                status = INCOMPLETE
            else:
                status = SUCCESSFUL

            up_jobits.append((status, id))
            up_jobs.append([status, host, return_code, retries] + times + data + [missed, id])
            if status == INCOMPLETE:
                for run, lumi in missed_lumis:
                    up_missed.append((FAILED, id, run, lumi))

        t = time.time()
        with self.db as db:
            db.executemany("""update jobits set
                status=?
                where job=?""",
                up_jobits)
            db.executemany("""update jobs set
                status=?,
                host=?,
                exit_code=?,
                retries=?,
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
                time_on_worker=?,
                time_total_on_worker=?,
                bytes_received=?,
                bytes_sent=?,
                missed_lumis=?
                where id=?""",
                up_jobs)
            db.executemany("update jobits set status=? where job=? and run=? and lumi=?",
                up_missed)
            for (dset, (num, complete, events)) in dsets.items():
                db.execute("""update datasets set
                    jobits_running=(jobits_running - ?),
                    jobits_done=(jobits_done + ?),
                    processed_events=(processed_events + ?)
                    where label=?""",
                    (dset, num, events, complete))
        db.commit()
        with open(os.path.join(self.config["workdir"], 'debug_sql_times'), 'a') as f:
            delta = time.time() - t
            size = len(jobs)
            ratio = delta / float(size) if size != 0 else 0
            f.write("RECV {0} {1} {2}\n".format(size, delta, ratio))

    def unfinished_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status!=?", (SUCCESSFUL,))
        return cur.fetchone()[0]

    def running_jobits(self):
        cur = self.db.execute("select count(*) from jobits where status==?", (ASSIGNED,))
        return cur.fetchone()[0]

    def dataset_info(self, label):
        cur = self.db.execute("""select dataset,
            path,
            release,
            global_tag,
            dbs_url,
            publish_label,
            cfg,
            pset_hash,
            id,
            uuid
            from datasets
            where label==?""", (label,))

        return cur.fetchone()

    def finished_jobs(self, dataset):
        cur = self.db.execute("""select id
            from jobs
            where status=?
            and dataset=?""", (SUCCESSFUL, dataset,))

        return cur.fetchall()

    def update_published(self, blocks):
        columns = [(PUBLISHED, block, id) for block, id in blocks]

        self.db.executemany("""update jobs
            set status=?,
            published_file_block=?
            where id=?""", columns)

        self.db.executemany("""update jobits
            set status=?
            where id=?""", [(x, z) for x, y, z in columns])

        self.db.commit()

    def update_datasets(self, column, value, label):
        self.db.execute("""update datasets
            set %s=?
            where label=?""" % column, (value, label,))

        self.db.commit()
