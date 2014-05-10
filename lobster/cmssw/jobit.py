from collections import defaultdict
import os
import random
import sqlite3
import time
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

def unique_lumis(lumis):
    """
    Count the unique lumi sections.  Lumis may be double-counted when
    they are split across several files.  This method should protect
    against that.

    >>> unique_lumis([(1, 1, 1, 3), (2, 1, 1, 4), (3, 2, 1, 4), (4, 3, 1, 5)])
    3

    >>> unique_lumis([(1, 1, -1, -1), (2, 2, -1, -1)])
    2
    """
    duplicates = 0
    unique_values = set()

    for (id, file, run, lumi) in lumis:
        if lumi > 0:
            if (run, lumi) in unique_values:
                duplicates += 1
            unique_values.add((run, lumi))

    return len(lumis) - duplicates

class JobitStore:
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
            publish_label text,
            pset_hash text default null,
            cfg text,
            uuid text,
            jobsize text,
            file_based int,
            jobits integer,
            jobits_running int default 0,
            jobits_done int default 0,
            jobits_left int default 0,
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
        except:
            pass

    def disconnect(self):
        self.db.close()

    def register(self, dataset_cfg, dataset_info):
        label = dataset_cfg['label']

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
                       file_based,
                       jobsize,
                       jobits,
                       jobits_left,
                       events)
                       values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                           dataset_cfg.get('dataset', dataset_cfg.get('files', None)),
                           label,
                           os.path.join(self.config['stageout location'], label),
                           os.path.basename(os.environ['LOCALRT']),
                           dataset_cfg.get('global tag'),
                           dataset_cfg.get('publish label', dataset_cfg['label']).replace('-', '_'), #TODO: more lexical checks #TODO: publish label check
                           dataset_cfg['cmssw config'],
                           self.uuid,
                           dataset_info.file_based,
                           dataset_info.jobsize,
                           dataset_info.total_lumis,
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

        self.db.execute("create index if not exists index_filename_{0} on files_{0}(filename)".format(label))
        self.db.execute("create index if not exists index_events_{0} on jobits_{0}(run, lumi)".format(label))
        self.db.execute("create index if not exists index_files_{0} on jobits_{0}(file)".format(label))

        self.db.commit()

    def pop_jobits(self, num=1, bijective=False):
        """
        Create a predetermined number of jobs.  The task these are
        created for is drawn randomly from all unfinished tasks.

        Arguments:
            num: the number of jobs to be created (default 1)
            bijective: process lumis from one file only, where possible (default False)
        Returns:
            a list containing an id, dataset label, file information (id,
            filename), lumi information (id, file id, run, lumi)
        """
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
                    order by skipped asc""".format(dataset)))
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

        # files and lumis for individual jobs
        files = set()
        lumis = set()

        # lumi veto to avoid duplicated processing
        all_lumis = set()

        # job container and current job size
        jobs = []
        current_size = 0

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
                all_lumis.add((run, lumi))
                for (ls_id, ls_file, ls_run, ls_lumi) in self.db.execute("""
                        select id, file, run, lumi
                        from jobits_{0}
                        where run=? and lumi=? and status not in (1, 2)""".format(dataset), (run, lumi)):
                    lumis.add((ls_id, ls_file, ls_run, ls_lumi))
                    files.add(ls_file)
            else:
                lumis.add((id, file, run, lumi))
                files.add(file)

            current_size += 1

            if current_size == size[0]:
                jobs.append((
                    str(job_id),
                    dataset,
                    [(id, fileinfo[id]) for id in files],
                    lumis))

                files = set()
                lumis = set()

                current_size = 0
                size.pop(0)

        if current_size > 0:
            jobs.append((
                str(job_id),
                dataset,
                [(id, fileinfo[id]) for id in files],
                lumis))

        dataset_update = []
        file_update = defaultdict(int)
        job_update = defaultdict(int)
        lumi_update = []

        for (job, label, files, lumis) in jobs:
            dataset_update += lumis
            job_update[job] = unique_lumis(lumis)
            lumi_update += [(job, id) for (id, file, run, lumi) in lumis]
            file_update[id] += unique_lumis(filter(lambda tpl: tpl[1] == file, lumis))

        self.db.execute(
                "update datasets set jobits_running=(jobits_running + ?) where id=?",
                (unique_lumis(dataset_update), dataset_id))

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
        job_updates = []
        dset_infos = {}

        for (dset, updates) in jobinfos.items():
            file_updates = []
            jobit_updates = []
            jobit_generic_updates = []

            for (job_update, file_update, jobit_update) in updates:
                # jobits either fail or are successful
                jobit_status = FAILED if job_update[-2] == FAILED else SUCCESSFUL
                job_updates.append(job_update)

                file_updates += file_update
                jobit_updates += jobit_update
                # the last entry in the job_update is the id
                jobit_generic_updates.append((jobit_status, job_update[-1]))

                try:
                    dset_infos[dset][0] += job_update[-4]
                    dset_infos[dset][1] += job_update[-3]
                except KeyError:
                    dset_infos[dset] = [job_update[-4], job_update[-3]]

            self.db.executemany("""update jobits_{0} set
                status=?
                where job=?""".format(dset),
                jobit_generic_updates)
            self.db.executemany("""update jobits_{0} set
                status=?
                where id=?""".format(dset),
                jobit_updates)

            self.db.executemany("""update files_{0} set
                jobits_running=(jobits_running - ?),
                jobits_done=(jobits_done + ?),
                events_read=(events_read + ?),
                skipped=(skipped + ?)
                where id=?""".format(dset),
                file_updates)

        self.db.executemany("""update jobs set
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
            events_written=?,
            status=?
            where id=?""",
            job_updates)

        for label in jobinfos.keys():
            self.update_dataset_stats(label, *dset_infos[label])

        self.db.commit()

    def update_dataset_stats(self, label, events_read=0, events_written=0):
        file_based = self.db.execute("select file_based from datasets where label=?", (label,)).fetchone()[0]

        if file_based:
            self.db.execute("""
                update datasets set
                    jobits_running=(select count(*) from jobits_{0} where status==1),
                    jobits_done=(select count(*) from jobits_{0} where status==2),
                    jobits_left=(select count(*) from jobits_{0} where status not in (1, 2)),
                    events_read=(events_read + ?),
                    events_written=(events_written + ?)
                where label=?""".format(label),
                (events_read, events_written, label))
        else:
            self.db.execute("""
                update datasets set
                    jobits_running=(select count(*) from (select distinct run, lumi from jobits_{0} where status==1)),
                    jobits_done=(select count(*) from (select distinct run, lumi from jobits_{0} where status==2)),
                    jobits_left=(select count(*) from (select distinct run, lumi from jobits_{0} where status not in (1, 2))),
                    events_read=(events_read + ?),
                    events_written=(events_written + ?)
                where label=?""".format(label),
                (events_read, events_written, label))

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
