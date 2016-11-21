from collections import Counter, defaultdict
import json
import logging
import math
import os
from retrying import retry
import sqlite3
import uuid

from lobster import util

logger = logging.getLogger('lobster.unit')

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

# Task type
PROCESS = 0
MERGE = 1

TaskUpdate = util.record('TaskUpdate',
                         'bytes_bare_output',
                         'bytes_output',
                         'bytes_received',
                         'bytes_sent',
                         'network_bandwidth',
                         'network_bytes_received',
                         'network_bytes_sent',
                         'allocated_cores',
                         'allocated_memory',
                         'allocated_disk',
                         'cache',
                         'cache_end_size',
                         'cache_start_size',
                         'cores',
                         'exit_code',
                         'events_read',
                         'events_written',
                         'host',
                         'units_processed',
                         'memory_resident',
                         'memory_swap',
                         'memory_virtual',
                         'status',
                         'time_submit',
                         'time_transfer_in_start',
                         'time_transfer_in_end',
                         'time_wrapper_start',
                         'time_wrapper_ready',
                         'time_stage_in_end',
                         'time_prologue_end',
                         'time_processing_end',
                         'time_epilogue_end',
                         'time_stage_out_end',
                         'time_transfer_out_start',
                         'time_transfer_out_end',
                         'time_retrieved',
                         'time_on_worker',
                         'time_total_on_worker',
                         'time_total_exhausted_execution',
                         'time_total_until_worker_failure',
                         'exhausted_attempts',
                         'time_cpu',
                         'workdir_footprint',
                         'workdir_num_files',
                         'id',
                         default=0)


class UnitStore:

    def __init__(self, config):
        self.uuid = str(uuid.uuid4()).replace('-', '')
        self.db_path = os.path.join(config.workdir, "lobster.db")
        self.db = sqlite3.connect(self.db_path, timeout=90)

        self.config = config

        self.db.execute("""create table if not exists workflows(
            cfg text,
            dataset text,
            events int default 0,
            file_based int,
            global_tag text,
            id integer primary key autoincrement,
            parent int default null,
            units integer,
            units_done int default 0,
            units_left int default 0,
            units_available int default 0,
            units_paused int default 0,
            units_running int default 0,
            taskruntime int default null,
            tasksize int,
            label text,
            units_masked int default 0,
            merged int default 0,
            path text,
            pset_hash text default null,
            publish_label text,
            release text,
            uuid text,
            transfers text default '{}',
            stop_on_file_boundary)""")
        self.db.execute("""create table if not exists tasks(
            bytes_bare_output int default 0 not null,
            bytes_output int default 0 not null,
            bytes_received int default 0 not null,
            bytes_sent int default 0 not null,
            network_bandwidth int default 0 not null,
            network_bytes_received int default 0 not null,
            network_bytes_sent int default 0 not null,
            cache int default 0 not null,
            cache_end_size int default 0 not null,
            cache_start_size int default 0 not null,
            workflow int not null,
            id integer primary key autoincrement,
            events_read int default 0 not null,
            events_written int default 0 not null,
            exit_code int default 0 not null,
            failed int default 0 not null,
            host text default '',
            task int default -1 not null,
            units int default 0 not null,
            units_processed int default 0 not null,
            memory_resident int default 0 not null,
            memory_virtual int default 0 not null,
            memory_swap int default 0 not null,
            cores int default 0 not null,
            allocated_cores int default 0 not null,
            allocated_memory int default 0 not null,
            allocated_disk int default 0 not null,
            published_file_block text,
            status int default 0 not null,
            time_submit int default 0 not null,
            time_transfer_in_start int default 0 not null,
            time_transfer_in_end int default 0 not null,
            time_wrapper_start int default 0 not null,
            time_wrapper_ready int default 0 not null,
            time_stage_in_end int default 0 not null,
            time_prologue_end int default 0 not null,
            time_processing_end int default 0 not null,
            time_epilogue_end int default 0 not null,
            time_stage_out_end int default 0 not null,
            time_transfer_out_start int default 0 not null,
            time_transfer_out_end int default 0 not null,
            time_retrieved int default 0 not null,
            time_on_worker int default 0 not null,
            time_total_on_worker int default 0 not null,
            time_total_exhausted_execution int default 0 not null,
            time_total_until_worker_failure int default 0 not null,
            exhausted_attempts int default 0 not null,
            time_cpu int default 0 not null,
            type int default 0 not null,
            workdir_footprint int default 0 not null,
            workdir_num_files int default 0 not null,
            foreign key(workflow) references workflows(id))""")

        self.db.execute("create index if not exists index_w_label on workflows(label)")
        self.db.execute("create index if not exists index_t_workflow on tasks(workflow, status)")
        self.db.execute("create index if not exists index_t_workflowplus on tasks(workflow, status, type)")

        self.db.commit()

    def disconnect(self):
        self.db.close()

    def max_taskid(self):
        maxid = self.db.execute(
            'select ifnull(max(id), 0) from tasks').fetchone()[0]
        return maxid

    def register_dataset(self, wflow, dataset_info, taskruntime=None):
        label = wflow.label
        unique_args = wflow.unique_arguments

        cur = self.db.cursor()
        cur.execute("""insert into workflows
                       (dataset,
                       label,
                       path,
                       release,
                       global_tag,
                       publish_label,
                       cfg,
                       uuid,
                       file_based,
                       tasksize,
                       taskruntime,
                       units,
                       units_masked,
                       units_left,
                       events,
                       stop_on_file_boundary
                       )
                       values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", (
            wflow.label,
            label,
            wflow.label,
            os.path.basename(os.environ.get('LOCALRT', '')),
            wflow.globaltag,
            wflow.publish_label,
            wflow.pset,
            self.uuid,
            dataset_info.file_based,
            dataset_info.tasksize,
            taskruntime,
            dataset_info.total_units * len(unique_args),
            dataset_info.masked_units,
            dataset_info.total_units * len(unique_args),
            dataset_info.total_events,
            getattr(dataset_info, 'stop_on_file_boundary', False)))

        self.db.execute("""create table if not exists files_{0}(
            id integer primary key autoincrement,
            filename text,
            skipped int default 0,
            units int,
            units_done int default 0,
            units_running int default 0,
            events int,
            events_read int default 0,
            bytes int default 0)""".format(label))

        cur.execute("""create table if not exists units_{0}(
            id integer primary key autoincrement,
            task integer,
            run integer,
            lumi integer,
            file integer,
            status integer default 0,
            failed integer default 0,
            arg text,
            foreign key(task) references tasks(id),
            foreign key(file) references files_{0}(id))""".format(label))

        self.db.execute("create index if not exists index_f_filename_{0} on files_{0}(filename)".format(label))
        self.db.execute("create index if not exists index_u_events_{0} on units_{0}(run, lumi)".format(label))
        self.db.execute("create index if not exists index_u_files_{0} on units_{0}(file, status)".format(label))
        self.db.execute("create index if not exists index_u_task_{0} on units_{0}(task)".format(label))
        self.db.commit()

        self.register_files(dataset_info.files, label, unique_args)

    def register_dependency(self, label, parent, total_units):
        with self.db as db:
            db.execute("""
                        update workflows
                        set
                            parent=(select id from workflows where label=?),
                            units=?
                        where label=?""", (parent, total_units, label)
                       )

    def register_files(self, infos, label, unique_args=None):
        with self.db as db:
            cur = db.cursor()

            if unique_args is None:
                unique_args = [None]

            update = []
            # Sort for reproducable unit tests.
            if len(infos) < 25:
                items = [(fn, infos[fn]) for fn in sorted(infos.keys())]
            else:
                items = infos.items()
            for fn, info in items:
                cur.execute(
                    """insert into files_{0}(units, events, filename, bytes) values (?, ?, ?, ?)""".format(
                        label),
                    (len(info.lumis) * len(unique_args), info.events, fn, info.size))
                fid = cur.lastrowid

                for arg in unique_args:
                    update += [(fid, run, lumi, arg)
                               for (run, lumi) in info.lumis]
            self.db.executemany(
                "insert into units_{0}(file, run, lumi, arg) values (?, ?, ?, ?)".format(label), update)
            self.update_workflow_stats(label)

    def work_left(self, label):
        """
        Get information about what is left to do for a workflow.

        Parameters
        ----------
            label : str
                The workflow for which to return the stats.

        Returns
        -------
            complete : bool
                If the whole workflow is available for processing.
            units_left : int
                How many units are left for the workflow - including
                unavailable ones.
            tasks_left : float
                How many tasks need to be created to process all units
                currently available.
        """
        complete, units_left, tasks_left = self.db.execute(
            "select (units_left = units_available), units_left, units_available * 1. / tasksize from workflows where label=?",
            (label,)).fetchone()
        return complete, units_left, tasks_left

    @retry(stop_max_attempt_number=10)
    def pop_units(self, workflow, num, taper=1.):
        """Create tasks from a workflow.

        Parameters
        ----------
            workflow : str
                The label of the workflow.
            num : int
                The number of tasks to create.
            taper : int
                Factor to apply to the tasksize.
        """
        with self.db:
            workflow_id, tasksize, stop_on_file_boundary = self.db.execute(
                "select id, tasksize, stop_on_file_boundary from workflows where label=?",
                (workflow,)).fetchone()

            logger.debug(("creating {0} task(s) for workflow {1}:" +
                          "\n\ttaper:    {4}" +
                          "\n\ttasksize: {5}" +
                          "\n\tthreshold for skipping: {2}" +
                          "\n\tthreshold for failure:  {3}").format(
                              num,
                              workflow,
                              self.config.advanced.threshold_for_skipping,
                              self.config.advanced.threshold_for_failure,
                              taper,
                              tasksize
            )
            )

            fileinfo = list(self.db.execute("""select id, filename
                        from files_{0}
                        where
                            (units_done + units_running < units) and
                            (skipped < ?)
                        order by skipped asc""".format(workflow), (self.config.advanced.threshold_for_skipping,)))
            files = [x for (x, y) in fileinfo]
            fileinfo = dict(fileinfo)

            tasksize = int(math.ceil(tasksize * taper))

            logger.debug("creating tasks with adjusted size {}".format(tasksize))

            rows = []
            for i in range(0, len(files), 40):
                chunk = files[i:i + 40]
                rows.extend(self.db.execute("""
                    select id, file, run, lumi, arg, failed
                    from units_{0}
                    where file in ({1}) and status not in (1, 2, 6, 7, 8)
                    order by file
                    """.format(workflow, ', '.join('?' for _ in chunk)), chunk))

            logger.debug("creating tasks from {} files, {} units".format(len(files), len(rows)))

            # files and lumis for individual tasks
            files = set()
            units = []

            # task container and current task size
            tasks = []
            current_size = 0

            def insert_task(files, units, arg):
                cur = self.db.cursor()
                cur.execute("insert into tasks(workflow, status, type) values (?, 1, 0)", (workflow_id,))
                task_id = cur.lastrowid

                tasks.append((
                    str(task_id),
                    workflow,
                    [(id, fileinfo[id]) for id in files],
                    units,
                    arg,
                    False))

            for id, file, run, lumi, arg, failed in rows:
                if failed > self.config.advanced.threshold_for_failure:
                    logger.debug("skipping run {}, "
                                 "lumi {} "
                                 "with failure count {} "
                                 "exceeding `config.advanced.threshold_for_failure={}`".format(
                                     run, lumi, failed, self.config.advanced.threshold_for_failure))
                    continue

                if failed == self.config.advanced.threshold_for_failure:
                    logger.debug("creating isolation task for run {}, lumi {} with failure count {}".format(
                        run, lumi, failed))
                    insert_task([file], [(id, file, run, lumi)], arg)
                    continue

                if stop_on_file_boundary and (len(files) == 1) and (file not in files):
                    insert_task(files, units, arg)

                    files = set()
                    units = []

                    current_size = 0
                    num -= 1

                # We are done creating tasks here, *if* we are about to
                # add the current unit to a new task, but have already
                # created enough tasks.
                if current_size == 0 and num <= 0:
                    break

                units.append((id, file, run, lumi))
                files.add(file)

                current_size += 1

                if current_size == tasksize:
                    insert_task(files, units, arg)

                    files = set()
                    units = []

                    current_size = 0
                    num -= 1

            if current_size > 0:
                insert_task(files, units, arg)

            workflow_update = []
            file_update = defaultdict(int)
            task_update = defaultdict(int)
            unit_update = []

            for (task, label, files, units, arg, merge) in tasks:
                workflow_update += units
                task_update[task] = len(units)
                unit_update += [(task, id) for (id, file, run, lumi) in units]
                for (id, filename) in files:
                    file_update[
                        id] += len(filter(lambda tpl: tpl[1] == id, units))

            self.db.execute(
                "update workflows set units_running=(units_running + ?) where id=?",
                (len(workflow_update), workflow_id))

            self.db.executemany("update files_{0} set units_running=(units_running + ?) where id=?".format(workflow),
                                [(v, k) for (k, v) in file_update.items()])
            self.db.executemany("update tasks set units=? where id=?",
                                [(v, k) for (k, v) in task_update.items()])
            self.db.executemany("update units_{0} set status=1, task=? where id=?".format(workflow),
                                unit_update)

            return tasks if len(unit_update) > 0 else []

    def reset_units(self):
        with self.db as db:
            ids = [id for (id,) in db.execute(
                "select id from tasks where status=1")]
            db.execute("update workflows set units_running=0, merged=0")
            db.execute("update tasks set status=4 where status=1")
            db.execute("update tasks set status=2 where status=7")
            for (label, dset_id) in db.execute("select label, id from workflows"):
                db.execute(
                    "update files_{0} set units_running=0".format(label))
                db.execute(
                    "update units_{0} set status=4 where status=1".format(label))
                db.execute(
                    "update units_{0} set status=2 where status=7".format(label))
                self.update_workflow_stats(label)
        return ids

    @retry(stop_max_attempt_number=10)
    def update_units(self, taskinfos):
        task_updates = []

        with self.db:
            for ((dset, unit_source), updates) in taskinfos.items():
                file_updates = []
                unit_updates = []
                unit_fail_updates = []
                unit_generic_updates = []

                for (task_update, file_update, unit_update) in updates:
                    task_updates.append(task_update)
                    file_updates += file_update

                    # units either fail or are successful
                    # FIXME this should really go into the task handler
                    if unit_source == 'tasks':
                        unit_status = SUCCESSFUL if task_update.status == FAILED else MERGED
                    else:
                        unit_status = FAILED if task_update.status == FAILED else SUCCESSFUL

                    if task_update.status == FAILED:
                        unit_fail_updates.append((task_update.id,))

                    unit_updates += unit_update
                    unit_generic_updates.append((unit_status, task_update.id))

                # update all units of the tasks
                self.db.executemany("""update {0} set
                    status=?
                    where task=?""".format(unit_source),
                                    unit_generic_updates)

                # update selected, missed units
                self.db.executemany("""update {0} set
                    status=?
                    where id=?""".format(unit_source),
                                    unit_updates)

                # increment failed counter
                if len(unit_fail_updates) > 0:
                    self.db.executemany("""update {0} set
                        failed=failed + 1
                        where task=?""".format(unit_source),
                                        unit_fail_updates)

                # update files in the workflow
                if len(file_updates) > 0:
                    self.db.executemany("""update files_{0} set
                        units_running=(select count(*) from units_{0} where file=files_{0}.id and status==1),
                        units_done=(select count(*) from units_{0} where file=files_{0}.id and status==2),
                        events_read=(events_read + ?),
                        skipped=(skipped + ?)
                        where id=?""".format(dset),
                                        file_updates)

            query = "update tasks set {0} where id=?".format(
                TaskUpdate.sql_fragment(stop=-1))
            self.db.executemany(query, task_updates)

            for label, _ in taskinfos.keys():
                self.update_workflow_stats(label)

    def update_workflow_stats_paused(self, roots=None):
        """Update workflow statistics after increasing thresholds.

        Happens only where needed, recursively traversing all dependency
        trees between workflows, issuing blanket updates if a parent
        workflow is changed.
        """
        if roots is None:
            roots = [w for w in self.config.workflows if not w.parent]
        for r in roots:
            merged, paused = self.db.execute(
                "select merged, units_paused from workflows where label=?", (r.label,)).fetchone()
            if paused > 0:
                for m in r.family():
                    self.db.execute(
                        "update workflows set merged=0 where label=?", (m.label,))
                    self.update_workflow_stats(m.label)
            elif len(r.dependents) > 0:
                self.update_workflow_stats(r.dependents)

    def update_workflow_runtime(self, updates):
        """Update workflow runtimes in the database.

        To synchronize runtimes present in the configuration with the ones
        in the database used for task size calculations.
        """
        with self.db:
            self.db.executemany(
                "update workflows set taskruntime=? where label=?", updates)

    def update_workflow_stats(self, label):
        id, size, targettime = self.db.execute(
            "select id, tasksize, taskruntime from workflows where label=?", (label,)).fetchone()

        if targettime is not None:
            # Adjust tasksize based on time spend in prologue, processing, and
            # epilogue.  Only do so when difference is > 10%
            tasks, unittime = self.db.execute("""
                select
                    count(*),
                    max(
                        avg((time_epilogue_end - time_stage_in_end) * 1. / units),
                        1
                    )
                from tasks where workflow=? and status in (2, 6, 7, 8) and type=0""", (id,)).fetchone()

            if tasks > 10:
                bettersize = max(1, int(math.ceil(targettime / unittime)))
                logger.debug("newly calculated task size for {}: {} (old: {})".format(
                    label, bettersize, size))
                if abs(float(bettersize - size) / size) > .05:
                    logger.info("adjusting task size for {0} from {1} to {2}".format(
                        label, size, bettersize))
                    self.db.execute(
                        "update workflows set tasksize=? where id=?", (bettersize, id))

        parent_paused = self.db.execute("""
            select
                ifnull((
                    case when parent is not null
                    then
                        (select units_paused from workflows where workflows.id == wf.parent)
                    else 0
                    end
                ), 0)
            from workflows as wf where id == ?
            """, (id,)).fetchone()[0]

        self.db.execute("""
            update workflows set
                units_running=ifnull((select count(*) from units_{0} where status == 1), 0),
                units_done=ifnull((select count(*) from units_{0} where status in (2, 6, 7, 8)), 0),
                units_paused=ifnull((
                        select count(*)
                        from units_{0}
                        where
                            (file in (select id from files_{0} where skipped >= ?) and status in (0, 3, 4)) or
                            (failed > ? and status in (0, 3, 4))
                    ), 0) + ?
            where label=?""".format(label),
                        (self.config.advanced.threshold_for_failure,
                         self.config.advanced.threshold_for_skipping, parent_paused, label,)
                        )

        self.db.execute("""
            update workflows set
                units_available=ifnull((select count(*) from units_{0}), 0) - (units_running + units_done + (units_paused - ?)),
                units_left=units - (units_running + units_done + units_paused)
            where label=?""".format(label), (parent_paused, label))

        if self.db.execute("select units_paused from workflows where label=?", (label,)).fetchone()[0] > 0:
            for (child,) in self.db.execute("select label from workflows where parent=?", (id,)):
                self.update_workflow_stats(child)

        if logger.getEffectiveLevel() <= logging.DEBUG:
            size, total, running, done, paused, available, left = self.db.execute("""
                select tasksize, units, units_running, units_done, units_paused, units_available, units_left
                from workflows where label=?""", (label,)).fetchone()
            logger.debug(("updated stats for {0}:\n\t" +
                          "tasksize:        {1}\n\t" +
                          "units total:     {7}\n\t" +
                          "units running:   {2}\n\t" +
                          "units done:      {3}\n\t" +
                          "units paused:    {4}\n\t" +
                          "units available: {5}\n\t" +
                          "units left:      {6}").format(label, size, running, done, paused, available, left, total))

    def merged(self):
        unmerged = self.db.execute(
            "select count(*) from workflows where merged <> 1").fetchone()[0]
        return unmerged == 0

    def estimate_tasks_left(self):
        rows = [xs for xs in self.db.execute("""
            select label, id, units_left, units_available * 1. / tasksize, tasksize
            from workflows
            where units_left > 0""")]
        if len(rows) == 0:
            return 0

        return sum(int(math.ceil(tasks)) for _, _, _, tasks, _ in rows)

    def unfinished_units(self):
        cur = self.db.execute(
            "select sum(units - units_done - units_paused) from workflows")
        res = cur.fetchone()[0]
        return 0 if res is None else res

    def running_units(self):
        cur = self.db.execute("select sum(units_running) from workflows")
        return cur.fetchone()[0]

    def workflow_info(self, label):
        cur = self.db.execute("""
            select
                dataset,
                path,
                release,
                global_tag,
                publish_label,
                cfg,
                pset_hash,
                id,
                uuid
            from workflows
            where label=?""", (label,))

        return cur.fetchone()

    def workflow_status(self):
        cursor = self.db.execute("""
            select
                label,
                events,
                ifnull((select sum(events_read) from tasks where workflow=workflows.id and status in (2, 6, 7, 8) and type=0), 0),
                ifnull((select sum(events_written) from tasks where workflow=workflows.id and status in (2, 6, 7, 8) and type=0), 0),
                units,
                units - units_masked,
                units_done,
                ifnull((select sum(units_processed) from tasks where workflow=workflows.id and status=8 and type=0), 0),
                units_paused,
                '' || round(
                        units_done * 100.0 / units,
                    1) || ' %',
                '' || ifnull(round(
                        ifnull((select sum(units_processed) from tasks where workflow=workflows.id and status=8 and type=0), 0) * 100.0 / units,
                    1), 0.0) || ' %'
            from workflows""")

        yield "Label Events read written Units unmasked written merged paused failed skipped Progress Merged".split()
        total = None
        for row in cursor:
            failed, skipped = self.db.execute("""
                select
                    ifnull((
                        select count(*)
                        from units_{0}
                        where failed > ? and status in (0, 3, 4)
                    ), 0),
                    ifnull((
                        select count(*)
                        from units_{0}
                        where file in (select id from files_{0} where skipped >= ?) and status in (0, 3, 4)
                    ), 0)
                from workflows where label=?
                """.format(row[0]), (self.config.advanced.threshold_for_failure, self.config.advanced.threshold_for_skipping, row[0])).fetchone()
            row = row[:-2] + (failed, skipped) + row[-2:]
            if total is None:
                total = list(row[1:-2])
            else:
                total = map(sum, zip(total, row[1:-2]))

            yield row
        yield ['Total'] + total + [
            '{} %'.format(round(total[-5] * 100. / total[-6], 1)),
            '{} %'.format(
                round(total[-4] * 100. / total[-5], 1) if total[-5] > 0 else 0.)
        ]

    @retry(stop_max_attempt_number=10)
    def pop_unmerged_tasks(self, workflow, bytes, num):
        """Method to get merge tasks.

        Parameters
        ----------
            workflow : str
                The label of the workflow to create merge tasks for.
            bytes : int
                The merge size of the workflow, in bytes.
            num : int
                How many merge tasks to create.
        """

        dset_id, merged = self.db.execute(
            "select id, merged from workflows where label=?", (workflow,)).fetchone()

        if merged:
            return []
        elif bytes <= 0:
            logger.debug("fully merged {0}".format(workflow))
            with self.db:
                self.db.execute(
                    """update workflows set merged=1 where id=?""", (dset_id,))
            return []

        mergeable, units_complete = self.db.execute("""
            select
                (
                    (select sum(bytes_bare_output) from tasks where workflow=workflows.id and status=2) > ?
                    or
                    units_done + units_paused == units
                )
                and
                (select count(*) from tasks where workflow=workflows.id and status=2) > 0,
                units_done + units_paused == units
            from workflows
            where label=?
        """, (bytes, workflow)).fetchone()

        if not mergeable:
            return []

        logger.debug("trying to merge tasks from {0}".format(workflow))

        class Merge(object):

            def __init__(self, task, units, size, maxsize):
                self.tasks = [task]
                self.units = units
                self.size = size
                self.maxsize = maxsize

            def __cmp__(self, other):
                return cmp(self.size, other.size)

            def add(self, task, units, size):
                if self.size + size > self.maxsize:
                    return False
                self.size += size
                self.units += units
                self.tasks.append(task)
                return True

            def left(self):
                return self.maxsize - self.size

        with self.db:
            # Select the finished processing tasks from the task
            rows = self.db.execute("""
                select id, units, bytes_bare_output
                from tasks
                where workflow=? and status=? and type=0
                order by bytes_bare_output desc""", (dset_id, SUCCESSFUL)).fetchall()

            # If we don't have enough rows, or the smallest two tasks can't be
            # merge, set this up so that the loop below is not evaluted and we
            # skip to the check if the merge for this workflow is complete for
            # the given maximum size.
            if len(rows) < 2 or rows[-2][1] + rows[-1][1] > bytes:
                rows = []
            else:
                minsize = rows[-1][1]

            candidates = []
            for task, units, size in rows:
                # Try to add the current task to a merge, in increasing order of
                # size left
                for merge in reversed(sorted(candidates)):
                    if merge.add(task, units, size):
                        break
                else:
                    # If we're too large to merge, we're skipped
                    if size + minsize <= bytes:
                        candidates.append(Merge(task, units, size, bytes))

            merges = []
            for merge in reversed(sorted(candidates)):
                if len(merge.tasks) == 1:
                    continue
                # For one iteration only: merge if we are either close enough
                # to the target size (TODO maybe this threshold should be
                # configurable? FIXME it's a magic number, anyways) or we are
                # done processing the task, when we merge everything we can.
                if units_complete or merge.size >= bytes * 0.9:
                    merges.append(merge)

            logger.debug("created {0} merge tasks".format(len(merges)))

            if len(merges) == 0 and units_complete:
                rows = self.db.execute(
                    """select count(*) from tasks where workflow=? and status=1""", (dset_id,)).fetchone()
                if rows[0] == 0:
                    logger.debug("fully merged {0}".format(workflow))
                    self.db.execute(
                        """update workflows set merged=1 where id=?""", (dset_id,))
                    return []

            res = []
            merge_update = []
            for merge in merges:
                merge_id = self.db.execute("""
                    insert into
                    tasks(workflow, units, status, type)
                    values (?, ?, ?, ?)""", (dset_id, merge.units, ASSIGNED, MERGE)).lastrowid
                logger.debug("inserted merge task {0} with tasks {1}".format(
                    merge_id, ", ".join(map(str, merge.tasks))))
                res += [(str(merge_id), workflow, [], [(id, None, -1, -1)
                                                       for id in merge.tasks], "", True)]
                merge_update += [(merge_id, id) for id in merge.tasks]

            if len(res) > 0:
                self.db.executemany(
                    "update tasks set status=7, task=? where id=?", merge_update)
                self.update_workflow_stats(workflow)

            return res

    def update_published(self, block):
        unmerged = [(name, task) for (name, task, merge_task) in block]
        unit_update = [task for (name, task, merge_task) in block]

        with self.db:
            self.db.executemany("""update tasks
                set status=6,
                published_file_block=?
                where id=?""", unmerged)

            self.db.executemany("""update tasks
                set status=6,
                published_file_block=?
                where task=?""", unmerged)

            for task, workflow in self.db.execute("""
                    select tasks.id, workflows.label
                    from tasks, workflows
                    where tasks.id in ({0}) and tasks.workflow=workflows.id""".format(", ".join(unit_update))):
                self.db.execute(
                    "update units_{0} set status=6 where task=?".format(workflow), (task,))

    def successful_tasks(self, label):
        dset_id = self.db.execute(
            "select id from workflows where label=?", (label,)).fetchone()[0]

        cur = self.db.execute("""
            select id, type
            from tasks
            where and workflow=? and status=2
            """, (dset_id,))

        return cur

    def merged_tasks(self, label):
        dset_id = self.db.execute(
            "select id from workflows where label=?", (label,)).fetchone()[0]

        cur = self.db.execute("""select id, type
            from tasks
            where workflow=? and status=8
            """, (dset_id,))

        return cur

    def failed_tasks(self, label):
        dset_id = self.db.execute(
            "select id from workflows where label=?", (label,)).fetchone()[0]
        cur = self.db.execute("""select id, type
            from tasks
            where status in (3, 4) and workflow=?
            """, (dset_id,))

        return cur

    def failed_units(self, label):
        tasks = self.db.execute("select task from units_{0} where failed > ?".format(
            label), (self.config.advanced.threshold_for_failure,))
        return [xs[0] for xs in tasks]

    def running_tasks(self):
        cur = self.db.execute("select id from tasks where status=1")
        for (v,) in cur:
            yield v

    def skipped_files(self, label):
        files = self.db.execute("select filename from files_{0} where skipped > ?".format(
            label), (self.config.advanced.threshold_for_skipping,))
        return [xs[0] for xs in files]

    def update_pset_hash(self, pset_hash, workflow):
        with self.db as conn:
            conn.execute(
                "update workflows set pset_hash=? where label=?", (pset_hash, workflow))

    @retry(stop_max_attempt_number=10)
    def update_missing(self, tasks):
        with self.db:
            for task, workflow in self.db.execute("""
                    select tasks.id, workflows.label
                    from tasks, workflows
                    where tasks.id in ({0}) and tasks.workflow=workflows.id""".format(", ".join(map(str, tasks)))):
                self.db.execute(
                    "update units_{0} set status=3 where task=?".format(workflow), (task,))

            # update tasks to be failed
            self.db.executemany("update tasks set status=3 where id=?", [
                                (task,) for task in tasks])
            # reset merged tasks from merging
            self.db.executemany("update tasks set status=2 where task=?", [
                                (task,) for task in tasks])

    def finished_files(self, infos):
        res = []
        for label, files in infos.items():
            for i in range(0, len(files), 999):
                chunk = list(files)[i:i + 999]
                res.extend(
                    self.db.execute(
                        """select filename
                        from files_{0}
                        where id in ({1}) and (units_done == units)""".format(label, ', '.join('?' for _ in chunk)), tuple(chunk)
                    )
                )

        return (x[0] for x in res)

    def update_transfers(self, transfers):
        for dataset in transfers:
            data = json.loads(self.db.execute(
                "select transfers from workflows where dataset=?", (dataset,)).fetchone()[0])

            for protocol in transfers[dataset]:
                if protocol in data:
                    transfers[dataset][protocol] += Counter(data[protocol])

            self.db.execute("update workflows set transfers=? where dataset=?", (json.dumps(
                transfers[dataset]), dataset))
