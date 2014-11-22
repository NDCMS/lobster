from collections import defaultdict
import multiprocessing
import os
import random
import sqlite3
import uuid

logger = multiprocessing.get_logger()

# FIXME these are hardcoded in some SQL statements below.  SQLite does not
# seem to have the concept of variables...

# Status
INITIALIZED = 0
ASSIGNED = 1
SUCCESSFUL = 2
FAILED = 3
ABORTED = 4
INCOMPLETE = 5
PUBLISHED = 6
MERGING = 7
MERGED = 8

# Job type
PROCESS = 0
MERGE = 1

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
            empty_source int,
            jobits integer,
            masked_lumis int default 0,
            jobits_running int default 0,
            jobits_done int default 0,
            jobits_left int default 0,
            unmerged int default 0,
            events int default 0)""")
        self.db.execute("""create table if not exists jobs(
            id integer primary key autoincrement,
            merge_job int,
            type int,
            host text,
            dataset int,
            published_file_block text,
            status int default 0,
            exit_code int,
            submissions int default 0,
            jobits int default 0,
            jobits_processed int default 0,
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
            time_processing_end int,
            time_chirp_end int,
            time_transfer_out_start int,
            time_transfer_out_end int,
            time_retrieved int,
            time_on_worker int,
            time_total_on_worker int,
            time_cpu int,
            bytes_received int,
            bytes_sent int,
            bytes_output int default 0,
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
        unique_args = dataset_cfg.get('unique parameters', [None])

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
                       empty_source,
                       jobsize,
                       jobits,
                       masked_lumis,
                       jobits_left,
                       events)
                       values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                           dataset_cfg.get('dataset', dataset_cfg.get('files', None)),
                           label,
                           os.path.join(self.config['stageout location'], label),
                           os.path.basename(os.environ['LOCALRT']),
                           dataset_cfg.get('global tag'),
                           dataset_cfg.get('publish label', dataset_cfg['label']).replace('-', '_'), #TODO: more lexical checks #TODO: publish label check
                           dataset_cfg.get('cmssw config'),
                           self.uuid,
                           dataset_info.file_based,
                           dataset_info.empty_source,
                           dataset_info.jobsize,
                           dataset_info.total_lumis * len(unique_args),
                           dataset_info.masked_lumis,
                           dataset_info.total_lumis * len(unique_args),
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
            events_read int default 0,
            bytes int default 0)""".format(label))

        cur.execute("""create table if not exists jobits_{0}(
            id integer primary key autoincrement,
            job integer,
            run integer,
            lumi integer,
            file integer,
            status integer default 0,
            arg text,
            foreign key(job) references jobs(id),
            foreign key(file) references files_{0}(id))""".format(label))

        for file in dataset_info.files:
            file_lumis = len(dataset_info.lumis[file])
            cur.execute(
                    """insert into files_{0}(jobits, events, filename, bytes) values (?, ?, ?, ?)""".format(label), (
                        file_lumis * len(unique_args),
                        dataset_info.event_counts[file],
                        file,
                        dataset_info.filesizes[file]))
            file_id = cur.lastrowid

            for arg in unique_args:
                columns = [(file_id, run, lumi, arg) for (run, lumi) in dataset_info.lumis[file]]
                self.db.executemany("insert into jobits_{0}(file, run, lumi, arg) values (?, ?, ?, ?)".format(label), columns)

        self.db.execute("create index if not exists index_filename_{0} on files_{0}(filename)".format(label))
        self.db.execute("create index if not exists index_events_{0} on jobits_{0}(run, lumi)".format(label))
        self.db.execute("create index if not exists index_files_{0} on jobits_{0}(file)".format(label))

        self.db.commit()

    def pop_jobits(self, num=1):
        """
        Create a predetermined number of jobs.  The task these are
        created for is drawn randomly from all unfinished tasks.

        Arguments:
            num: the number of jobs to be created (default 1)
        Returns:
            a list containing an id, dataset label, file information (id,
            filename), lumi information (id, file id, run, lumi)
        """

        rows = [xs for xs in self.db.execute("""
            select label, id, jobits - jobits_done - jobits_running, jobsize, empty_source
            from datasets
            where jobits_done + jobits_running < jobits""")]
        if len(rows) == 0:
            return None

        dataset, dataset_id, remaining, jobsize, empty_source = random.choice(rows)
        size = [int(jobsize)] * num

        fileinfo = list(self.db.execute("""select id, filename
                    from files_{0}
                    where jobits_done + jobits_running < jobits
                    order by skipped asc""".format(dataset)))
        files = [x for (x, y) in fileinfo]
        fileinfo = dict(fileinfo)

        rows = []
        for i in range(0, len(files), 40):
            chunk = files[i:i + 40]
            rows.extend(self.db.execute("""
                select id, file, run, lumi, arg
                from jobits_{0}
                where file in ({1}) and status not in (1, 2, 6, 7, 8)
                """.format(dataset, ', '.join('?' for _ in chunk)), chunk))

        # files and lumis for individual jobs
        files = set()
        jobits = []

        # lumi veto to avoid duplicated processing
        all_lumis = set()

        # job container and current job size
        jobs = []
        current_size = 0

        for id, file, run, lumi, arg in rows:
            if (run, lumi) in all_lumis:
                continue

            if current_size == 0:
                if len(size) == 0:
                    break
                cur = self.db.cursor()
                cur.execute("insert into jobs(dataset, status, type) values (?, 1, 0)", (dataset_id,))
                job_id = cur.lastrowid

            if lumi > 0:
                all_lumis.add((run, lumi))
                for (ls_id, ls_file, ls_run, ls_lumi) in self.db.execute("""
                        select id, file, run, lumi
                        from jobits_{0}
                        where run=? and lumi=? and status not in (1, 2, 6, 7, 8)""".format(dataset), (run, lumi)):
                    jobits.append((ls_id, ls_file, ls_run, ls_lumi))
                    files.add(ls_file)
            else:
                jobits.append((id, file, run, lumi))
                files.add(file)

            current_size += 1

            if current_size == size[0]:
                jobs.append((
                    str(job_id),
                    dataset,
                    [(id, fileinfo[id]) for id in files],
                    jobits,
                    arg,
                    empty_source))

                files = set()
                jobits = []

                current_size = 0
                size.pop(0)

        if current_size > 0:
            jobs.append((
                str(job_id),
                dataset,
                [(id, fileinfo[id]) for id in files],
                jobits,
                arg,
                empty_source))

        dataset_update = []
        file_update = defaultdict(int)
        job_update = defaultdict(int)
        jobit_update = []

        for (job, label, files, jobits, arg, empty_source) in jobs:
            dataset_update += jobits
            job_update[job] = len(jobits)
            jobit_update += [(job, id) for (id, file, run, lumi) in jobits]
            for (id, filename) in files:
                file_update[id] += len(filter(lambda tpl: tpl[1] == id, jobits))

        self.db.execute(
                "update datasets set jobits_running=(jobits_running + ?) where id=?",
                (len(dataset_update), dataset_id))

        self.db.executemany("update files_{0} set jobits_running=(jobits_running + ?) where id=?".format(dataset),
                [(v, k) for (k, v) in file_update.items()])
        self.db.executemany("update jobs set jobits=? where id=?",
                [(v, k) for (k, v) in job_update.items()])
        self.db.executemany("update jobits_{0} set status=1, job=? where id=?".format(dataset),
                jobit_update)

        self.db.commit()

        return jobs if len(jobit_update) > 0 else None

    def reset_jobits(self):
        with self.db as db:
            ids = [id for (id,) in db.execute("select id from jobs where status=1")]
            db.execute("update datasets set jobits_running=0")
            db.execute("update jobs set status=4 where status=1")
            db.execute("update jobs set status=2 where status=7 and type=0")
            db.execute("update jobs set status=4 where status=7 and type=1")
            db.execute("update jobs set status=2 where status=8 and type=1")
            for (label, dset_id) in db.execute("select label, id from datasets"):
                db.execute("update files_{0} set jobits_running=0".format(label))
                db.execute("update jobits_{0} set status=4 where status=1".format(label))
                db.execute("update jobits_{0} set status=2 where status=7".format(label))
                db.execute("""
                    update datasets set unmerged=(
                        select count(*)
                        from jobs
                        where status=2
                        and dataset=?)
                    where label=?""", (dset_id, label))
                self.update_dataset_stats(label)

        db.commit()

        return ids

    def update_jobits(self, jobinfos):
        job_updates = []

        for (dset, updates) in jobinfos.items():
            file_updates = []
            jobit_updates = []
            jobit_generic_updates = []

            for (job_update, file_update, jobit_update) in updates:
                job_updates.append(job_update)
                file_updates += file_update

                # jobits either fail or are successful
                jobit_status = FAILED if job_update[-2] == FAILED else SUCCESSFUL

                jobit_updates += jobit_update
                # the last entry in the job_update is the id
                jobit_generic_updates.append((jobit_status, job_update[-1]))

            # update all jobits of the jobs
            self.db.executemany("""update jobits_{0} set
                status=?
                where job=?""".format(dset),
                jobit_generic_updates)

            # update selected, missed jobits
            self.db.executemany("""update jobits_{0} set
                status=?
                where id=?""".format(dset),
                jobit_updates)

            # update files in the dataset
            self.db.executemany("""update files_{0} set
                jobits_running=(select count(*) from jobits_{0} where status==1 and file=files_{0}.id),
                jobits_done=(select count(*) from jobits_{0} where status==2 and file=files_{0}.id),
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
            time_processing_end=?,
            time_chirp_end=?,
            time_transfer_out_start=?,
            time_transfer_out_end=?,
            time_retrieved=?,
            time_on_worker=?,
            time_total_on_worker=?,
            time_cpu=?,
            bytes_received=?,
            bytes_sent=?,
            bytes_output=?,
            jobits_processed=(jobits - ?),
            events_read=?,
            events_written=?,
            status=?
            where id=?""",
            job_updates)

        for label in jobinfos.keys():
            self.update_dataset_stats(label)

        self.db.commit()

    def update_dataset_stats(self, label):
        self.db.execute("""
            update datasets set
                jobits_running=(select count(*) from jobits_{0} where status in (1, 7)),
                jobits_done=(select count(*) from jobits_{0} where status in (2, 6, 8)),
                jobits_left=(select count(*) from jobits_{0} where status not in (1, 2, 6, 7, 8)),
                unmerged=(select count(*) from jobs where status=2 and dataset=id)
            where label=?""".format(label), (label,))

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
            where label=?""", (label,))

        return cur.fetchone()

    def update_merged(self, jobinfos):
        merge_job_update = []
        process_job_success_update = []
        process_job_fail_update = []
        for (dset, updates) in jobinfos.items():
            merge_job_update += sum([x[0] for x in updates], [])
            process_job_success_update += sum([x[1] for x in updates], [])
            process_job_fail_update += sum([x[2] for x in updates], [])

            self.update_dataset_stats(dset)

        self.db.executemany("update jobs set status=?, bytes_output=? where id=?", merge_job_update)
        self.db.executemany("update jobs set status=?, merge_job=? where id=?", process_job_success_update)
        self.db.executemany("update jobs set status=? where id=?", process_job_fail_update)

        self.db.commit()

    def unfinished_merging(self):
        cur = self.db.execute("select count(*) from jobs where status in (2, 7)")

        return cur.fetchone()[0]

    def pop_unmerged_jobs(self, max_megabytes, num=1):
        """Create merging jobs.

        Adds jobs to the set to be merged, keeping a running total of
        output sizes until the next file would exceed the
        size specified by max_megabytes. For each set of jobs to be merged,
        creates a new entry of type MERGE in the job table.

        """

        max_bytes = max_megabytes * 1000000

        rows = self.db.execute("select label, id from datasets where unmerged > 0").fetchall()

        if len(rows) == 0:
            return None

        dataset, dset_id = random.choice(rows)

        merge_groups = []
        rows = self.db.execute("""
            select id, type, bytes_output
            from jobs
            where status=? and dataset=?
            order by bytes_output desc""", (SUCCESSFUL, dset_id))

        # FIXME this approach does not make any attempt to reduce mixing
        # of input files-- do we need to map input to output better?
        # FIXME all of this gymnastics is in an attempt to reduce the number
        # of queries-- we could better match the requested output size by adding
        # a repeated query for "max(size) < max_bytes - size"...
        # is it worth the processing expense?
        chunk = []
        for attempts in range(3):
            if len(merge_groups) > num:
                break

            tails = []
            size = 0

            for job, job_type, bytes_output in rows:
                print 'job type bytes ', job, job_type, bytes_output
                if (size + bytes_output) < max_bytes:
                    chunk += [(job, job_type)]
                    size += bytes_output
                else:
                    if len(chunk) > 1:
                        merge_groups += [chunk]
                        chunk = [(job, job_type)]
                        size = bytes_output
                    else:
                        tails += [(job, job_type, bytes_output)]

            random.shuffle(tails)
            rows = tails

        if len(chunk) > 1:
            merge_groups += [chunk]
        elif len(chunk) == 1:
            tails += [(job, job_type, 0) for job, job_type in chunk]

        res = []
        merge_update = []
        tail_update = [(job,) for job, job_type, bytes_output in tails]
        for chunk in merge_groups:
            merge_id = self.db.execute("""
                insert into
                jobs(dataset, status, type)
                values (?, ?, ?)""", (dset_id, ASSIGNED, MERGE)).lastrowid
            res += [(merge_id, dataset, chunk)]
            merge_update += [(job,) for job, job_type in chunk]

        self.db.executemany("update jobs set status=7 where id=?", merge_update)
        self.db.executemany("update jobs set status=8 where id=?", tail_update)

        self.db.execute("update datasets set unmerged=(unmerged - ?) where label=?", (len(tail_update), dataset))
        self.db.execute("update datasets set unmerged=(unmerged + ?) where label=?", (len(res), dataset))
        self.update_dataset_stats(dataset)

        self.db.commit()

        return res

    def update_published(self, block):
        unmerged = [(name, job) for (name, job, merge_job) in block]
        merged = [(name, merge_job) for (name, job, merge_job) in block]
        jobit_update = [job for (name, job, merge_job) in block]

        self.db.executemany("""update jobs
            set status=6,
            published_file_block=?
            where id=?""", unmerged)

        self.db.executemany("""update jobs
            set status=6,
            published_file_block=?
            where merge_job=?""", unmerged)

        for job, dataset in self.db.execute("""select jobs.id,
            datasets.label
            from jobs, datasets
            where jobs.id in ({0})
            and jobs.dataset=datasets.id""".format(", ".join(jobit_update))):
            self.db.execute("update jobits_{0} set status=6 where job=?".format(dataset), (job,))

        self.db.commit()

    def success_jobs(self, label):
        dset_id = self.db.execute("select id from datasets where label=?", (label,)).fetchone()[0]

        cur = self.db.execute("""
            select id, type
            from jobs
            where status in (2, 8) and dataset=?
            """, (dset_id,))

        return cur

    def fail_jobs(self, label):
        dset_id = self.db.execute("select id from datasets where label=?", (label,)).fetchone()[0]

        cur = self.db.execute("""select id, type
            from jobs
            where status in (3, 4, 5) and dataset=?
            """, (dset_id,))

        return cur

    def update_pset_hash(self, pset_hash, dataset):
        self.db.execute("update datasets set pset_hash=? where label=?", (pset_hash, dataset))

        self.db.commit()

    def update_missing(self, jobs):
        for job, dataset in self.db.execute("""select jobs.id,
            datasets.label
            from jobs, datasets
            where jobs.id in ({0})
            and jobs.dataset=datasets.id""".format(", ".join([str(j) for j, t in jobs]))):
            self.db.execute("update jobs set status=3 where id=?", (job,))
            self.db.execute("update jobits_{0} set status=3 where job=?".format(dataset), (job,))

        self.db.commit()

