from collections import defaultdict
import math
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
PUBLISHED = 6
MERGING = 7
MERGED = 8

# Job type
PROCESS = 0
MERGE = 1

class JobitStore:
    def __init__(self, config):
        self.uuid = str(uuid.uuid4()).replace('-', '')
        self.db_path = os.path.join(config['workdir'], "lobster.db")
        self.db = sqlite3.connect(self.db_path)

        self.__failure_threshold = config.get("threshold for failure", 10)
        self.__skipping_threshold = config.get("threshold for skipping", 10)

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
            jobsize int,
            jobruntime int default null,
            file_based int,
            empty_source int,
            jobits integer,
            masked_lumis int default 0,
            jobits_running int default 0,
            jobits_done int default 0,
            jobits_left int default 0,
            jobits_paused int default 0,
            events int default 0,
            merged int default 0)""")
        self.db.execute("""create table if not exists jobs(
            id integer primary key autoincrement,
            job int,
            type int,
            host text,
            dataset int,
            published_file_block text,
            status int default 0,
            failed int default 0,
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
            time_stage_in_end int,
            time_prologue_end int,
            time_file_requested int,
            time_file_opened int,
            time_file_processing int,
            time_processing_end int,
            time_epilogue_end int,
            time_stage_out_end int,
            time_transfer_out_start int,
            time_transfer_out_end int,
            time_retrieved int,
            time_on_worker int,
            time_total_on_worker int,
            time_cpu int,
            cache_start_size int,
            cache_end_size int,
            cache int,
            bytes_received int,
            bytes_sent int,
            bytes_output int default 0,
            bytes_bare_output int default 0,
            foreign key(dataset) references datasets(id))""")

        self.db.commit()

        try:
            cur = self.db.execute("select max(id) from jobs")
            count = int(cur.fetchone()[0])
        except:
            pass

    def disconnect(self):
        self.db.close()

    def register(self, dataset_cfg, dataset_info, filemap, taskruntime=None):
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
                       jobruntime,
                       jobits,
                       masked_lumis,
                       jobits_left,
                       events)
                       values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
                           dataset_cfg.get('dataset', label),
                           label,
                           dataset_info.path,
                           os.path.basename(os.environ['LOCALRT']),
                           dataset_cfg.get('global tag'),
                           dataset_cfg.get('publish label', dataset_cfg['label']).replace('-', '_'), #TODO: more lexical checks #TODO: publish label check
                           dataset_cfg.get('cmssw config'),
                           self.uuid,
                           dataset_info.file_based,
                           dataset_info.empty_source,
                           dataset_info.jobsize,
                           taskruntime,
                           dataset_info.total_lumis * len(unique_args),
                           dataset_info.masked_lumis,
                           dataset_info.total_lumis * len(unique_args),
                           dataset_info.total_events))

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
            failed integer default 0,
            arg text,
            foreign key(job) references jobs(id),
            foreign key(file) references files_{0}(id))""".format(label))

        for file in dataset_info.files:
            file_lumis = len(dataset_info.lumis[file])
            cur.execute(
                    """insert into files_{0}(jobits, events, filename, bytes) values (?, ?, ?, ?)""".format(label), (
                        file_lumis * len(unique_args),
                        dataset_info.event_counts[file],
                        filemap(file),
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
            select label, id, jobits_left, jobits_left * 1. / jobsize, jobsize, empty_source
            from datasets
            where jobits_left > 0""")]
        if len(rows) == 0:
            return []

        # calculate how many tasks we can create from all datasets, still
        tasks_left = sum(int(math.ceil(tasks)) for _, _, _, tasks, _, _ in rows)
        tasks = []

        random.shuffle(rows)

        # if total tasks left < requested tasks, make the tasks smaller to
        # keep all workers occupied
        if tasks_left < num:
            taper = float(tasks_left) / num
            for dataset, dataset_id, jobits_left, ntasks, jobsize, empty_source in rows:
                jobsize = max(math.ceil((taper * jobsize)), 1)
                size = [int(jobsize)] * max(1, int(math.ceil(ntasks / taper)))
                tasks.extend(self.__pop_jobits(size, dataset, dataset_id, empty_source))
        else:
            for dataset, dataset_id, jobits_left, ntasks, jobsize, empty_source in rows:
                size = [int(jobsize)] * max(1, int(math.ceil(ntasks * num / tasks_left)))
                tasks.extend(self.__pop_jobits(size, dataset, dataset_id, empty_source))
        return tasks

    def __pop_jobits(self, size, dataset, dataset_id, empty_source):
        """Internal method to create jobs from a dataset
        """
        logger.debug("creating {0} task(s) for workflow {1}".format(len(size), dataset))

        fileinfo = list(self.db.execute("""select id, filename
                    from files_{0}
                    where
                        (jobits_done + jobits_running < jobits) and
                        (skipped < ?)
                    order by skipped asc""".format(dataset), (self.__skipping_threshold,)))
        files = [x for (x, y) in fileinfo]
        fileinfo = dict(fileinfo)

        rows = []
        for i in range(0, len(files), 40):
            chunk = files[i:i + 40]
            rows.extend(self.db.execute("""
                select id, file, run, lumi, arg, failed
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

        def insert_job(files, jobits, arg):
            cur = self.db.cursor()
            cur.execute("insert into jobs(dataset, status, type) values (?, 1, 0)", (dataset_id,))
            job_id = cur.lastrowid

            jobs.append((
                str(job_id),
                dataset,
                [(id, fileinfo[id]) for id in files],
                jobits,
                arg,
                empty_source,
                False))

        for id, file, run, lumi, arg, failed in rows:
            if (run, lumi) in all_lumis or failed > self.__failure_threshold:
                continue

            if current_size == 0:
                if len(size) == 0:
                    break

            if failed == self.__failure_threshold:
                insert_job([file], [(id, file, run, lumi)], arg)
                continue

            if lumi > 0:
                all_lumis.add((run, lumi))
                for (ls_id, ls_file, ls_run, ls_lumi) in self.db.execute("""
                        select
                            id, file, run, lumi
                        from
                            jobits_{0}
                        where
                            run=? and
                            lumi=? and
                            status not in (1, 2, 6, 7, 8) and
                            failed < ?""".format(dataset),
                        (run, lumi, self.__failure_threshold)):
                    jobits.append((ls_id, ls_file, ls_run, ls_lumi))
                    files.add(ls_file)
            else:
                jobits.append((id, file, run, lumi))
                files.add(file)

            current_size += 1

            if current_size == size[0]:
                insert_job(files, jobits, arg)

                files = set()
                jobits = []

                current_size = 0
                size.pop(0)

        if current_size > 0:
            insert_job(files, jobits, arg)

        dataset_update = []
        file_update = defaultdict(int)
        job_update = defaultdict(int)
        jobit_update = []

        for (job, label, files, jobits, arg, empty_source, merge) in jobs:
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

        return jobs if len(jobit_update) > 0 else []

    def reset_jobits(self):
        with self.db as db:
            ids = [id for (id,) in db.execute("select id from jobs where status=1")]
            db.execute("update datasets set jobits_running=0, merged=0")
            db.execute("update jobs set status=4 where status=1")
            db.execute("update jobs set status=2 where status=7")
            for (label, dset_id) in db.execute("select label, id from datasets"):
                db.execute("update files_{0} set jobits_running=0".format(label))
                db.execute("update jobits_{0} set status=4 where status=1".format(label))
                db.execute("update jobits_{0} set status=2 where status=7".format(label))
                self.update_dataset_stats(label)

        db.commit()

        return ids

    def update_jobits(self, jobinfos):
        job_updates = []

        for ((dset, jobit_source), updates) in jobinfos.items():
            file_updates = []
            jobit_updates = []
            jobit_fail_updates = []
            jobit_generic_updates = []

            for (job_update, file_update, jobit_update) in updates:
                job_updates.append(job_update)
                file_updates += file_update

                # jobits either fail or are successful
                # FIXME this should really go into the job handler
                if jobit_source == 'jobs':
                    jobit_status = SUCCESSFUL if job_update[-2] == FAILED else MERGED
                else:
                    jobit_status = FAILED if job_update[-2] == FAILED else SUCCESSFUL

                if job_update[-2] == FAILED:
                    jobit_fail_updates.append((job_update[-1],))

                jobit_updates += jobit_update
                # the last entry in the job_update is the id
                jobit_generic_updates.append((jobit_status, job_update[-1]))

            # update all jobits of the jobs
            self.db.executemany("""update {0} set
                status=?
                where job=?""".format(jobit_source),
                jobit_generic_updates)

            # update selected, missed jobits
            self.db.executemany("""update {0} set
                status=?
                where id=?""".format(jobit_source),
                jobit_updates)

            # increment failed counter
            if len(jobit_fail_updates) > 0:
                self.db.executemany("""update {0} set
                    failed=failed + 1
                    where job=?""".format(jobit_source),
                    jobit_fail_updates)

            # update files in the dataset
            if len(file_updates) > 0:
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
            time_stage_in_end=?,
            time_prologue_end=?,
            time_file_requested=?,
            time_file_opened=?,
            time_file_processing=?,
            time_processing_end=?,
            time_epilogue_end=?,
            time_stage_out_end=?,
            time_transfer_out_start=?,
            time_transfer_out_end=?,
            time_retrieved=?,
            time_on_worker=?,
            time_total_on_worker=?,
            time_cpu=?,
            cache_start_size=?,
            cache_end_size=?,
            cache=?,
            bytes_received=?,
            bytes_sent=?,
            bytes_output=?,
            bytes_bare_output=?,
            jobits_processed=(jobits - ?),
            events_read=?,
            events_written=?,
            status=?
            where id=?""",
            job_updates)

        for label, _ in jobinfos.keys():
            self.update_dataset_stats(label)

        self.db.commit()

    def update_dataset_stats(self, label):
        id, size, targettime = self.db.execute("select id, jobsize, jobruntime from datasets where label=?", (label,)).fetchone()

        if targettime is not None:
            # Adjust jobsize based on time spend in prologue, processing, and
            # epilogue.  Only do so when difference is > 10%
            tasks, jobittime = self.db.execute("""
                select
                    count(*),
                    avg((time_epilogue_end - time_stage_in_end) * 1. / jobits)
                from jobs where status in (2, 6, 7, 8) and dataset=1 and type=0""").fetchone()

            if tasks > 10:
                bettersize = max(1, int(math.ceil(targettime / jobittime)))
                if abs(float(bettersize - size) / size) > .1:
                    logger.info("adjusting task size for {0} from {1} to {2}".format(label, size, bettersize))
                    self.db.execute("update datasets set jobsize=? where id=?", (bettersize, id))

        self.db.execute("""
            update datasets set
                jobits_running=(select count(*) from jobits_{0} where status in (1, 7)),
                jobits_done=(select count(*) from jobits_{0} where status in (2, 6, 8)),
                jobits_paused=
                    (select count(*) from jobits_{0} where failed > ?) +
                    ifnull((select sum(jobits - jobits_done) from files_{0} where skipped >= ?), 0),
                jobits_left=jobits - (jobits_running + jobits_done + jobits_paused)
            where label=?""".format(label), (self.__failure_threshold, self.__skipping_threshold, label))

    def merged(self):
        unmerged = self.db.execute("select count(*) from datasets where merged <> 1").fetchone()[0]
        return unmerged == 0

    def unfinished_jobits(self):
        cur = self.db.execute("select sum(jobits - jobits_done - jobits_paused) from datasets")
        res = cur.fetchone()[0]
        return 0 if res is None else res

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

    def pop_unmerged_jobs(self, bytes, num=1):
        """Create merging jobs.

        This creates `num` merge jobs with a maximal size of `bytes`.
        """

        if bytes <= 0:
            return []

        rows = self.db.execute("""
            select label, id, jobits_done + jobits_paused == jobits
            from datasets
            where
                merged <> 1 and
                (jobits_done + jobits_paused) * 10 >= jobits
                and (select count(*) from jobs where dataset=datasets.id and status=2) > 0
        """).fetchall()

        if len(rows) == 0:
            logger.debug("no merge possibility found")
            return []

        dataset, dset_id, jobits_complete = random.choice(rows)

        logger.debug("trying to merge jobs from {0}".format(dataset))

        # Select the finished processing jobs from the task
        rows = self.db.execute("""
            select id, jobits, bytes_bare_output
            from jobs
            where status=? and dataset=? and type=0
            order by bytes_bare_output desc""", (SUCCESSFUL, dset_id)).fetchall()

        # If we don't have enough rows, or the smallest two jobs can't be
        # merge, set this up so that the loop below is not evaluted and we
        # skip to the check if the merge for this dataset is complete for
        # the given maximum size.
        if len(rows) < 2 or rows[-2][1] + rows[-1][1] > bytes:
            rows = []
        else:
            minsize = rows[-1][1]

        class Merge(object):
            def __init__(self, job, jobits, size, maxsize):
                self.jobs = [job]
                self.jobits = jobits
                self.size = size
                self.maxsize = maxsize
            def __cmp__(self, other):
                return cmp(self.size, other.size)
            def add(self, job, jobits, size):
                if self.size + size > self.maxsize:
                    return False
                self.size += size
                self.jobits += jobits
                self.jobs.append(job)
                return True
            def left(self):
                return self.maxsize - self.size

        candidates = []
        for job, jobits, size in rows:
            # Try to add the current job to a merge, in increasing order of
            # size left
            for merge in reversed(sorted(candidates)):
                if merge.add(job, jobits, size):
                    break
            else:
                # If we're too large to merge, we're skipped
                if size + minsize <= bytes:
                    candidates.append(Merge(job, jobits, size, bytes))

        merges = []
        for merge in reversed(sorted(candidates)):
            if len(merge.jobs) == 1:
                continue
            # For one iteration only: merge if we are either close enough
            # to the target size (TODO maybe this threshold should be
            # configurable? FIXME it's a magic number, anyways) or we are
            # done processing the task, when we merge everything we can.
            if jobits_complete or merge.size >= bytes * 0.9:
                merges.append(merge)

        logger.debug("created {0} merge jobs".format(len(merges)))

        if len(merges) == 0 and jobits_complete:
            rows = self.db.execute("""select count(*) from jobs where status=1 and dataset=?""", (dset_id,)).fetchone()
            if rows[0] == 0:
                logger.debug("fully merged {0}".format(dataset))
                self.db.execute("""update datasets set merged=1 where id=?""", (dset_id,))
                self.db.commit()
                return []

        res = []
        merge_update = []
        for merge in merges:
            merge_id = self.db.execute("""
                insert into
                jobs(dataset, jobits, status, type)
                values (?, ?, ?, ?)""", (dset_id, merge.jobits, ASSIGNED, MERGE)).lastrowid
            logger.debug("inserted merge job {0} with jobs {1}".format(merge_id, ", ".join(map(str, merge.jobs))))
            res += [(str(merge_id), dataset, [], [(id, None, -1, -1) for id in merge.jobs], "", False, True)]
            merge_update += [(merge_id, id) for id in merge.jobs]

        self.db.executemany("update jobs set status=7, job=? where id=?", merge_update)
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
            where job=?""", unmerged)

        for job, dataset in self.db.execute("""select jobs.id,
            datasets.label
            from jobs, datasets
            where jobs.id in ({0})
            and jobs.dataset=datasets.id""".format(", ".join(jobit_update))):
            self.db.execute("update jobits_{0} set status=6 where job=?".format(dataset), (job,))

        self.db.commit()

    def successful_jobs(self, label):
        dset_id = self.db.execute("select id from datasets where label=?", (label,)).fetchone()[0]

        cur = self.db.execute("""
            select id, type
            from jobs
            where status=2 and dataset=?
            """, (dset_id,))

        return cur

    def merged_jobs(self, label):
        dset_id = self.db.execute("select id from datasets where label=?", (label,)).fetchone()[0]

        cur = self.db.execute("""select id, type
            from jobs
            where status=8 and dataset=?
            """, (dset_id,))

        return cur

    def failed_jobs(self, label):
        dset_id = self.db.execute("select id from datasets where label=?", (label,)).fetchone()[0]

        cur = self.db.execute("""select id, type
            from jobs
            where status in (3, 4) and dataset=?
            """, (dset_id,))

        return cur

    def running_jobs(self):
        cur = self.db.execute("select id from jobs where status=1")
        for (v,) in cur:
            yield v

    def update_pset_hash(self, pset_hash, dataset):
        self.db.execute("update datasets set pset_hash=? where label=?", (pset_hash, dataset))

        self.db.commit()

    def update_missing(self, jobs):
        for job, dataset in self.db.execute("""select jobs.id,
            datasets.label
            from jobs, datasets
            where jobs.id in ({0})
            and jobs.dataset=datasets.id""".format(", ".join(map(str, jobs)))):
            self.db.execute("update jobits_{0} set status=3 where job=?".format(dataset), (job,))

        # update jobs to be failed
        self.db.executemany("update jobs set status=3 where id=?", [(job,) for job in jobs])
        # reset merged jobs from merging
        self.db.executemany("update jobs set status=2 where job=?", [(job,) for job in jobs])

        self.db.commit()

