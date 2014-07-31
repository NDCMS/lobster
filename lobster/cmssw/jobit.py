from collections import defaultdict
import os
import random
import sqlite3
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
MERGING = 7

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
            jobits_running int default 0,
            jobits_done int default 0,
            jobits_left int default 0,
            events int default 0,
            events_read int default 0,
            events_written int default 0,
            bytes_input int default 0,
            bytes_output int default 0)""")
        self.db.execute("""create table if not exists jobs(
            id integer primary key autoincrement,
            merged_job int default 0,
            merging_job int default 0,
            merge_status int default 0,
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

        self.db.execute("""create table if not exists merge_jobs(
            id integer primary key autoincrement,
            status int default 0,
            bytes_output int default 0)""")

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
                       jobits_left,
                       events,
                       bytes_input)
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
                           dataset_info.total_lumis * len(unique_args),
                           dataset_info.total_events,
                           sum(dataset_info.filesizes.values())))
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
        if bijective:
            remaining = sum(size)
            size = []
            for file in files:
                lumis = self.db.execute("""
                    select id, file, run, lumi, arg
                    from jobits_{0}
                    where file=? and (status<>1 and status<>2 and status<>6)""".format(dataset), (file,)).fetchall()
                size.append(len(lumis))
                remaining -= len(lumis)
                if remaining <= 0:
                    break
                rows += lumis
        else:
            for i in range(0, len(files), 40):
                chunk = files[i:i + 40]
                rows.extend(self.db.execute("""
                    select id, file, run, lumi, arg
                    from jobits_{0}
                    where file in ({1}) and (status<>1 and status<>2 and status<>6)
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
                cur.execute("insert into jobs(dataset, status) values (?, 1)", (dataset_id,))
                job_id = cur.lastrowid

            if lumi > 0:
                all_lumis.add((run, lumi))
                for (ls_id, ls_file, ls_run, ls_lumi) in self.db.execute("""
                        select id, file, run, lumi
                        from jobits_{0}
                        where run=? and lumi=? and status not in (1, 2, 6)""".format(dataset), (run, lumi)):
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
                job_updates.append(job_update)
                file_updates += file_update

                # jobits either fail or are successful
                jobit_status = FAILED if job_update[-2] == FAILED else SUCCESSFUL

                jobit_updates += jobit_update
                # the last entry in the job_update is the id
                jobit_generic_updates.append((jobit_status, job_update[-1]))

                # bytes written, events read and written
                try:
                    dset_infos[dset][0] += job_update[-6]
                    dset_infos[dset][1] += job_update[-4]
                    dset_infos[dset][2] += job_update[-3]
                except KeyError:
                    dset_infos[dset] = [job_update[-6], job_update[-4], job_update[-3]]

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
            self.update_dataset_stats(label, *dset_infos[label])

        self.db.commit()

    def update_dataset_stats(self, label, bytes_written=0, events_read=0, events_written=0):
        file_based = self.db.execute("select file_based from datasets where label=?", (label,)).fetchone()[0]

        self.db.execute("""
            update datasets set
                jobits_running=(select count(*) from jobits_{0} where status==1),
                jobits_done=(select count(*) from jobits_{0} where status==2),
                jobits_left=(select count(*) from jobits_{0} where status not in (1, 2)),
                events_read=(events_read + ?),
                events_written=(events_written + ?),
                bytes_output=(bytes_output + ?)
            where label=?""".format(label),
            (events_read, events_written, bytes_written, label))

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

    def reset_merging(self):
        self.db.execute("update merge_jobs set status=? where status=?", (FAILED, MERGING))
        self.db.execute("update jobs set merge_status=?, merging_job=?", (INITIALIZED, None))

        self.db.commit()

    def update_merged(self, jobs):
        success_update = [(merging_job, status, None, job) for (job, merging_job, status, size) in jobs if status == SUCCESSFUL]
        merge_jobs_update = [(status, size, merging_job) for (job, merging_job, status, size) in jobs]
        fail_update = [merging_job for (job, merging_job, status, size) in jobs if status == FAILED]


        self.db.executemany("update merge_jobs set status=?, bytes_output=? where id=?", merge_jobs_update)
        self.db.executemany("update jobs set merged_job=?, merge_status=?, merging_job=? where id=?", success_update)

        for job in fail_update:
            cur = self.db.execute("insert into merge_jobs(status) values (?)", (ASSIGNED,))
            self.db.execute("update jobs set merging_job=?, merge_status=? where merging_job=?", (cur.lastrowid, ASSIGNED, job))

        self.db.commit()

    def unfinished_merging(self):
        cur = self.db.execute("select count(distinct merging_job) from jobs")

        return cur.fetchone()[0]

    def register_unmerged(self, max_megabytes=3500):
        max_bytes = max_megabytes * 1000000

        for dataset in self.db.execute("select id from datasets"):
            cur = self.db.execute("insert into merge_jobs(status) values (?)", (ASSIGNED,))
            size = 0
            chunk = []
            rows = self.db.execute("""select jobs.id,
                jobs.merged_job,
                jobs.bytes_output
                from jobs left join merge_jobs
                on jobs.merged_job=merge_jobs.id
                where jobs.status=?
                and jobs.dataset=?
                and merge_jobs.id is null
                union
                select jobs.id,
                jobs.merged_job,
                merge_jobs.bytes_output
                from merge_jobs left join jobs
                on jobs.merged_job=merge_jobs.id
                where merge_jobs.status=?
                and jobs.dataset=?
                group by merge_jobs.id
                order by jobs.id""", (SUCCESSFUL, dataset[0], SUCCESSFUL, dataset[0])).fetchall()

            for job, merged_job, bytes in rows:
                if (size + bytes) < max_bytes:
                    chunk += [(cur.lastrowid, ASSIGNED, job)]
                    size += bytes
                    if id == rows[-1][1] and len(chunk) > 1:
                        self.db.executemany("update jobs set merging_job=?, merge_status=? where id=?", chunk)
                        cur = self.db.execute("insert into merge_jobs(status) values (?)", (ASSIGNED,))
                else:
                    if len(chunk) > 1:
                        self.db.executemany("update jobs set merging_job=?, merge_status=? where id=?", chunk)
                        cur = self.db.execute("insert into merge_jobs(status) values (?)", (ASSIGNED,))
                    chunk += [(cur.lastrowid, ASSIGNED, job)]
                    size = bytes

        self.db.commit()

    def pop_unmerged_jobs(self, max=1):
        res = []
        for id, dataset in self.db.execute("""select distinct merging_job,
            label
            from jobs
            join datasets
            on datasets.id=jobs.dataset
            where merge_status=?
            or merge_status=?
            limit ?""", (ASSIGNED, FAILED, max)).fetchall():

            cur = self.db.execute("select id, merged_job from jobs where merging_job=?", (id,))
            res += [(id, dataset, cur.fetchall())]

            self.db.execute("update jobs set merge_status=? where merging_job=?", (MERGING, id))
            self.db.execute("update merge_jobs set status=? where id=?", (MERGING, id))

        self.db.commit()

        return res

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


    def finished_jobs(self, dataset):
        cur = self.db.execute("""select jobs.id, jobs.merged_job
            from jobs, datasets
            where jobs.status=?
            and datasets.label=?
            and jobs.dataset==datasets.id""", (SUCCESSFUL, dataset))

        return cur.fetchall()
