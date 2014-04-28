from collections import defaultdict
import os
import random
import sqlite3
import time
import uuid

from FWCore.PythonUtilities.LumiList import LumiList

from dataset import MetaInterface

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

        self.__interface = MetaInterface()

        # Use four databases: one for jobits, jobs, hosts, datasets each
        self.db.execute("""create table if not exists datasets(
            id integer primary key autoincrement,
            dataset text,
            label text,
            path text,
            release text,
            global_tag text,
            publish_label text,
            pset_hash text default null,
            cfg text,
            uuid text,
            jobsize text,
            jobits integer,
            jobits_running int default 0,
            jobits_done int default 0,
            events int default 0,
            events_read int default 0,
            events_written int default 0)""")
        self.db.execute("""create table if not exists jobs(
            id integer primary key autoincrement,
            host text,
            dataset int,
            published_file_block text,
            status int default 0,
            exit_code int,
            submissions int default 0,
            jobits int default 0,
            jobits_processed int default 0,
            jobits_missed int default 0,
            events_read int default 0,
            events_written int default 0,
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

    def register_jobits(self, cfg):
        label = cfg['label']

        print "Querying {0}...".format(label)

        dataset_info = self.__interface.get_info(cfg)

        print "Registering {0}...".format(label)

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
                       publish_label,
                       cfg,
                       uuid,
                       jobsize,
                       jobits,
                       events)
                       values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                           cfg['dataset'],
                           label,
                           os.path.join(self.config['stageout location'], label),
                           os.path.basename(os.environ['LOCALRT']),
                           cfg.get('global tag'),
                           cfg.get('publish label', cfg['label']).replace('-', '_'), #TODO: more lexical checks #TODO: publish label check
                           cfg['cmssw config'],
                           self.uuid,
                           dataset_info.jobsize,
                           dataset_info.total_lumis,
                           dataset_info.total_events))
        dset_id = cur.lastrowid

        self.db.execute("""create table if not exists files_{0}(
            id integer primary key autoincrement,
            filename text,
            skipped int default 0,
            jobits int,
            jobits_done int default 0,
            jobits_running int default 0,
            events int,
            events_read int default 0)""".format(label))

        cur.execute("""create table if not exists jobits_{0}(
            id integer primary key autoincrement,
            job integer,
            run integer,
            lumi integer,
            file integer,
            status integer default 0,
            foreign key(job) references jobs(id),
            foreign key(file) references files(id))""".format(label))

        for file in dataset_info.files:
            file_lumis = len(dataset_info.lumis[file])
            cur.execute("""insert into files_{0}(jobits, events, filename) values (?, ?, ?)""".format(label),
                    (file_lumis, dataset_info.event_counts[file], file))
            file_id = cur.lastrowid

            columns = [(file_id, run, lumi) for (run, lumi) in dataset_info.lumis[file]]
            self.db.executemany("insert into jobits_{0}(file, run, lumi) values (?, ?, ?)".format(label), columns)

        # self.db.execute("create index if not exists index_skipped_{0} on files_{0}(skipped)".format(label))
        self.db.execute("create index if not exists index_filename_{0} on files_{0}(filename)".format(label))
        # self.db.execute("create index if not exists index_events_{0} on jobits_{0}(run, lumi)".format(label))
        self.db.execute("create index if not exists index_events_{0} on jobits_{0}(run, lumi)".format(label))
        self.db.execute("create index if not exists index_files_{0} on jobits_{0}(file)".format(label))
        # self.db.execute("create index if not exists dataset_index on jobits(dataset)")
        # self.db.execute("create index if not exists ex on jobits_{0}(dataset, file asc)")
        # self.db.execute("create index if not exists nfile_index on jobits(attempts, file)")

        self.db.commit()

    def pop_jobits(self, num=1, bijective=False):
        t = time.time()

        rows = [xs for xs in self.db.execute("""
            select label, id, jobits - jobits_done - jobits_running, jobsize
            from datasets
            where jobits_done + jobits_running < jobits""")]
        if len(rows) == 0:
            return None

        dataset, dataset_id, remaining, jobsize = random.choice(rows)
        size = [int(jobsize)] * num

        fileinfo = list(self.db.execute("""select id, filename
                    from files_{0}
                    where jobits_done + jobits_running < jobits
                    order by skipped""".format(dataset)))
        files = [x for (x, y) in fileinfo]
        fileinfo = dict(fileinfo)

        rows = []
        if bijective:
            remaining = sum(size)
            size = []
            for file in files:
                lumis = self.db.execute("""
                    select id, file, run, lumi
                    from jobits_{0}
                    where file=? and (status<>1 and status<>2 and status<>6)""".format(dataset), (file,))
                size.append(len(lumis))
                remaining -= len(lumis)
                if remaining <= 0:
                    break
                rows += lumis
        else:
            for i in range(0, len(files), 40):
                chunck = files[i:i + 40]
                rows.extend(self.db.execute("""
                    select id, file, run, lumi
                    from jobits_{0}
                    where file in ({1}) and (status<>1 and status<>2 and status<>6)
                    """.format(dataset, ', '.join('?' for _ in chunck)), chunck))

        files = set()
        lumis = set()
        all_lumis = set()
        jobs = []
        job_update = {}
        lumi_update = []
        file_update = defaultdict(int)
        current_size = 0
        total_lumis = 0

        for id, file, run, lumi in rows:
            if (run, lumi) in all_lumis:
                continue

            if current_size == 0:
                if len(size) == 0:
                    break
                cur = self.db.cursor()
                cur.execute("insert into jobs(dataset, status) values (?, 1)", (dataset_id,))
                job_id = cur.lastrowid

            if lumi > 0:
                lumis.add((run, lumi))
                all_lumis.add((run, lumi))
                for (ls_id, ls_file) in self.db.execute("""
                        select id, file
                        from jobits_{0}
                        where run=? and lumi=?""".format(dataset), (run, lumi)):
                    lumi_update.append((job_id, ls_id))
                    files.add(ls_file)
                    file_update[ls_file] += 1
            else:
                lumi_update.append((job_id, id))
                files.add(file)
                file_update[file] += 1

            current_size += 1

            if current_size == size[0]:
                job_update[job_id] = len(lumis)
                jobs.append((
                    str(job_id),
                    dataset,
                    [fileinfo[id] for id in files],
                    LumiList(lumis=lumis)))

                total_lumis += len(lumis)

                size.pop(0)
                files = set()
                lumis = set()
                current_size = 0

        if current_size > 0:
            job_update[job_id] = len(lumis)
            jobs.append((
                str(job_id),
                dataset,
                [fileinfo[id] for id in files],
                LumiList(lumis=lumis)))

            total_lumis += len(lumis)

        self.db.execute(
                "update datasets set jobits_running=(jobits_running + ?) where id=?",
                (total_lumis, dataset_id))

        self.db.executemany("update files_{0} set jobits_running=(jobits_running + ?) where id=?".format(dataset),
                [(v, k) for (k, v) in file_update.items()])
        self.db.executemany("update jobs set jobits=? where id=?",
                [(v, k) for (k, v) in job_update.items()])
        self.db.executemany("update jobits_{0} set status=1, job=? where id=?".format(dataset),
                lumi_update)

        self.db.commit()

        with open(os.path.join(self.config["workdir"], 'debug_sql_times'), 'a') as f:
            delta = time.time() - t
            size = len(jobs)
            ratio = delta / float(size) if size != 0 else 0
            f.write("CREA {0} {1} {2}\n".format(size, delta, ratio))

        return jobs if len(lumi_update) > 0 else None

    def reset_jobits(self):
        with self.db as db:
            ids = [id for (id,) in db.execute("select id from jobs where status=1")]
            db.execute("update datasets set jobits_running=0")
            db.execute("update jobs set status=4 where status=1")
            for (label,) in db.execute("select label from datasets"):
                db.execute("update files_{0} set jobits_running=0".format(label))
                db.execute("update jobits_{0} set status=4 where status=1".format(label))
        return ids

    def update_jobits(self, jobinfos):
        t = time.time()
        up_jobs = []

        dsets = {}
        for (dset, jobs) in jobinfos.items():
            up_jobits = []
            up_missed = []
            up_read = defaultdict(int)
            up_skipped = defaultdict(int)

            jobids = []

            for (id, host, failed, return_code, submissions, \
                    processed_lumis, missed_lumis, skipped_files, \
                    times, data, read, written) in jobs:
                id = int(id)
                jobids.append(id)

                missed = len(missed_lumis)
                processed = len(processed_lumis)

                try:
                    dsets[dset][0] += missed + processed
                    dsets[dset][1] += processed
                    dsets[dset][2] += sum(read.values())
                    dsets[dset][3] += written
                except KeyError:
                    dsets[dset] = [missed + processed, processed, sum(read.values()), written]

                if failed:
                    job_status = FAILED
                    jobit_status = FAILED
                elif missed > 0:
                    job_status = INCOMPLETE
                    jobit_status = SUCCESSFUL
                    for run, lumi in missed_lumis:
                        up_missed.append((FAILED, run, lumi))
                else:
                    job_status = SUCCESSFUL
                    jobit_status = SUCCESSFUL

                for file in skipped_files:
                    up_skipped[file] += 1

                for (file, count) in read.items():
                    up_read[file] += count

                up_jobs.append([job_status, host, return_code, submissions] + times + data + [processed, missed, sum(read.values()), written, id])
                up_jobits.append((jobit_status, id))

            self.db.executemany("""update jobits_{0} set
                status=?
                where job=?""".format(dset),
                up_jobits)
            self.db.executemany("""update jobits_{0} set
                status=?
                where run=? and lumi=?""".format(dset),
                up_missed)

            up_processed = defaultdict(int)
            up_done = defaultdict(int)

            for id in jobids:
                for (file_id, count) in self.db.execute("""select file, count(*)
                        from jobits_{0}
                        where job=?
                        group by file""".format(dset),
                        (id,)):
                    up_processed[file_id] += count
                for (file_id, count) in self.db.execute("""select file, count(*)
                        from jobits_{0}
                        where job=? and status=?
                        group by file""".format(dset),
                        (id, SUCCESSFUL)):
                    up_done[file_id] += count

            self.db.executemany("""update files_{0} set
                jobits_running=(jobits_running - ?)
                where id=?""".format(dset),
                [(v, k) for (k, v) in up_processed.items()])
            self.db.executemany("""update files_{0} set
                jobits_done=(jobits_done + ?)
                where id=?""".format(dset),
                [(v, k) for (k, v) in up_done.items()])
            self.db.executemany("""update files_{0} set
                events_read=(events_read + ?)
                where filename=?""".format(dset),
                [(v, k) for (k, v) in up_read.items()])
            self.db.executemany("""update files_{0} set
                skipped=(skipped + ?)
                where filename=?""".format(dset),
                [(v, k) for (k, v) in up_skipped.items()])

        self.db.executemany("""update jobs set
            status=?,
            host=?,
            exit_code=?,
            submissions=?,
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
            jobits_processed=?,
            jobits_missed=?,
            events_read=?,
            events_written=?
            where id=?""",
            up_jobs)
        for (dset, (num, complete, read, written)) in dsets.items():
            self.db.execute("""update datasets set
                jobits_running=(jobits_running - ?),
                jobits_done=(jobits_done + ?),
                events_read=(events_read + ?),
                events_written=(events_written + ?)
                where label=?""",
                (num, complete, read, written, dset))
        self.db.commit()

        with open(os.path.join(self.config["workdir"], 'debug_sql_times'), 'a') as f:
            delta = time.time() - t
            size = len(jobs)
            ratio = delta / float(size) if size != 0 else 0
            f.write("RECV {0} {1} {2}\n".format(size, delta, ratio))

    def unfinished_jobits(self):
        cur = self.db.execute("select sum(jobits - jobits_done) from datasets")
        return cur.fetchone()[0]

    def running_jobits(self):
        cur = self.db.execute("select sum(jobits_running) from datasets")
        return cur.fetchone()[0]

    def dataset_info(self, label):
        cur = self.db.execute("""select dataset,
            path,
            release,
            global_tag,
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
