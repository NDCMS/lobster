import daemon
import datetime
import inspect
import logging
import logging.handlers
import os
import psutil
import resource
import signal
import sys
import time
import traceback

from lobster import actions, util
from lobster.commands.status import Status
from lobster.core.command import Command
from lobster.core.source import TaskProvider

import work_queue as wq

logger = logging.getLogger('lobster.core')


class Terminate(Command):

    @property
    def help(self):
        return 'terminate running lobster instance'

    def setup(self, argparser):
        pass

    def run(self, args):
        self.kill(args.config)

    def kill(self, config):
        logger.info("setting flag to quit at the next checkpoint")
        logger.debug("the following stack trace doesn't indicate a crash; it's just for debugging purposes.")
        logger.debug("stack:\n{0}".format(''.join(traceback.format_stack())))
        util.register_checkpoint(config.workdir, 'KILLED', 'PENDING')

        if config.elk:
            config.elk.end()


class Process(Command, util.Timing):

    def __init__(self):
        util.Timing.__init__(self, 'action', 'create', 'fetch', 'return', 'status', 'update')

    @property
    def help(self):
        return "process configuration"

    @property
    def daemonizable(self):
        return True

    def additional_logs(self):
        return ['configure']

    def setup_logging(self, category):
        filename = os.path.join(self.config.workdir, "lobster_stats_{}.log".format(category))
        if not hasattr(self, 'log_attributes'):
            self.log_attributes = [m for (m, o) in inspect.getmembers(wq.work_queue_stats)
                                   if not inspect.isroutine(o) and not m.startswith('__')]

        with open(filename, "a") as statsfile:
            statsfile.write(
                " ".join(
                    ["#timestamp", "units_left"] +
                    ["total_{}_time".format(k) for k in sorted(self.times.keys())] +
                    ["total_source_{}_time".format(k) for k in sorted(self.source.times.keys())] +
                    self.log_attributes
                ) + "\n"
            )

    def log(self, category, left):
        filename = os.path.join(self.config.workdir, "lobster_stats_{}.log".format(category))
        if category == 'all':
            stats = self.queue.stats_hierarchy
        else:
            stats = self.queue.stats_category(category)

        with open(filename, "a") as statsfile:
            now = datetime.datetime.now()
            statsfile.write(" ".join(map(str,
                                         [int(int(now.strftime('%s')) * 1e6 + now.microsecond), left] +
                                         [self.times[k] for k in sorted(self.times.keys())] +
                                         [self.source.times[k] for k in sorted(self.source.times.keys())] +
                                         [getattr(stats, a) for a in self.log_attributes]
                                         )) + "\n"
                            )

        if self.config.elk:
            stats = self.queue.stats_hierarchy
            self.config.elk.index_stats(now, left, self.times, self.log_attributes, stats, category)

    def setup(self, argparser):
        argparser.add_argument('--finalize', action='store_true', default=False,
                               help='do not process any additional data; wrap project up by merging everything')
        argparser.add_argument('--foreground', action='store_true', default=False,
                               help='do not daemonize; run in the foreground instead')
        argparser.add_argument('-f', '--force', action='store_true', default=False,
                               help='force processing, even if the working directory is locked by a previous instance')

    def run(self, args):
        self.config = args.config

        if args.finalize:
            args.config.advanced.threshold_for_failure = 0
            args.config.advanced.threshold_for_skipping = 0

        if not os.path.exists(self.config.workdir):
            os.makedirs(self.config.workdir)

        if not util.checkpoint(self.config.workdir, "version"):
            util.register_checkpoint(
                self.config.workdir, "version", util.get_version())
        else:
            util.verify(self.config.workdir)

        if not args.foreground:
            ttyfile = open(os.path.join(self.config.workdir, 'process.err'), 'a')
            logger.info("saving stderr and stdout to {0}".format(
                os.path.join(self.config.workdir, 'process.err')))
            args.preserve.append(ttyfile)

        if self.config.advanced.dump_core:
            logger.info("setting core dump size to unlimited")
            resource.setrlimit(resource.RLIMIT_CORE, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

        def localkill(num, frame):
            Terminate().run(args)

        signals = daemon.daemon.make_default_signal_map()
        signals[signal.SIGINT] = localkill
        signals[signal.SIGTERM] = localkill

        process = psutil.Process()
        preserved = [f.name for f in args.preserve]
        preserved += [os.path.realpath(os.path.abspath(f)) for f in preserved]
        openfiles = [f for f in process.open_files() if f.path not in preserved]
        openconns = process.connections()

        for c in openconns:
            logger.debug("open connection: {}".format(c))
            args.preserve.append(c.fd)

        if len(openfiles) > 0:
            logger.error("cannot daemonize due to open files")
            for f in openfiles:
                logger.error("open file: {}".format(f.path))
            raise RuntimeError("open files or connections")

        with daemon.DaemonContext(
                detach_process=not args.foreground,
                stdout=sys.stdout if args.foreground else ttyfile,
                stderr=sys.stderr if args.foreground else ttyfile,
                files_preserve=args.preserve,
                working_directory=self.config.workdir,
                pidfile=util.get_lock(self.config.workdir, args.force),
                prevent_core=False,
                initgroups=False,
                signal_map=signals):
            self.sprint()

            logger.info("lobster terminated")
            if not args.foreground:
                logger.info("stderr and stdout saved in {0}".format(
                    os.path.join(self.config.workdir, 'process.err')))

            try:
                # Fails if something with working directory creation went wrong
                Status().run(args)
            except Exception:
                pass

    def sprint(self):
        with util.PartiallyMutable.unlock():
            self.source = TaskProvider(self.config)
        action = actions.Actions(self.config, self.source)

        logger.info("using wq from {0}".format(wq.__file__))
        logger.info("running Lobster version {0}".format(util.get_version()))
        logger.info("current PID is {0}".format(os.getpid()))

        wq.cctools_debug_flags_set("all")
        wq.cctools_debug_config_file(os.path.join(self.config.workdir, "work_queue_debug.log"))
        wq.cctools_debug_config_file_size(1 << 29)

        self.queue = wq.WorkQueue(-1)
        self.queue.specify_min_taskid(self.source.max_taskid() + 1)
        self.queue.specify_log(os.path.join(self.config.workdir, "work_queue.log"))
        self.queue.specify_transactions_log(os.path.join(self.config.workdir, "transactions.log"))
        self.queue.specify_name("lobster_" + self.config.label)
        self.queue.specify_keepalive_timeout(300)
        # self.queue.tune("short-timeout", 600)
        self.queue.tune("transfer-outlier-factor", 4)
        self.queue.specify_algorithm(wq.WORK_QUEUE_SCHEDULE_RAND)
        self.queue.enable_monitoring_snapshots("snapshots_trigger.log")
        if self.config.advanced.full_monitoring:
            self.queue.enable_monitoring_full(None)
        else:
            self.queue.enable_monitoring(None)

        logger.info("starting queue as {0}".format(self.queue.name))

        abort_active = False
        abort_threshold = self.config.advanced.abort_threshold
        abort_multiplier = self.config.advanced.abort_multiplier

        wq_max_retries = self.config.advanced.wq_max_retries

        if util.checkpoint(self.config.workdir, 'KILLED') == 'PENDING':
            util.register_checkpoint(self.config.workdir, 'KILLED', 'RESTART')

        # time in seconds to wait for WQ to return tasks, with minimum wait
        # time in case no more tasks are waiting
        interval = 120
        interval_minimum = 30

        tasks_left = 0
        units_left = 0
        successful_tasks = 0

        categories = []

        self.setup_logging('all')
        # Workflows can be assigned categories, with each category having
        # different cpu/memory/walltime requirements that WQ will automatically
        # fine-tune
        for category in self.config.categories:
            constraints = category.wq()
            if category.name != 'merge':
                categories.append(category.name)
                self.setup_logging(category.name)
            self.queue.specify_category_mode(category.name, category.mode)
            if category.mode == wq.WORK_QUEUE_ALLOCATION_MODE_FIXED:
                self.queue.specify_category_max_resources(category.name, constraints)
            else:
                self.queue.specify_category_first_allocation_guess(category.name, constraints)
            logger.debug('Category {0}: {1}'.format(category.name, constraints))
            if 'wall_time' not in constraints:
                self.queue.activate_fast_abort_category(category.name, abort_multiplier)

        proxy_email_sent = False
        while not self.source.done():
            with self.measure('status'):
                tasks_left = self.source.tasks_left()
                units_left = self.source.work_left()

                logger.debug("expecting {0} tasks, still".format(tasks_left))
                self.queue.specify_num_tasks_left(tasks_left)

                for c in categories + ['all']:
                    self.log(c, units_left)

                if util.checkpoint(self.config.workdir, 'KILLED') == 'PENDING':
                    util.register_checkpoint(
                        self.config.workdir, 'KILLED', str(datetime.datetime.utcnow()))

                    # let the task source shut down gracefully
                    logger.info("terminating task source")
                    self.source.terminate()
                    logger.info("terminating gracefully")
                    break

            with self.measure('create'):
                have = {}
                for c in categories:
                    cstats = self.queue.stats_category(c)
                    have[c] = {'running': cstats.tasks_running, 'queued': cstats.tasks_waiting}

                stats = self.queue.stats_hierarchy
                tasks = self.source.obtain(stats.total_cores, have)

                expiry = None
                if self.config.advanced.proxy:
                    expiry = self.config.advanced.proxy.expires()
                    proxy_time_left = self.config.advanced.proxy.time_left()
                    if proxy_time_left >= 24 * 3600:
                        proxy_email_sent = False
                    if proxy_time_left < 24 * 3600 and not proxy_email_sent:
                        util.sendemail("Your proxy is about to expire.\n" + "Timeleft: " + str(datetime.timedelta(seconds=proxy_time_left)), self.config)
                        proxy_email_sent = True

                for category, cmd, id, inputs, outputs, env, dir in tasks:
                    task = wq.Task(cmd)
                    task.specify_category(category)
                    task.specify_tag(id)
                    task.specify_max_retries(wq_max_retries)
                    task.specify_monitor_output(os.path.join(dir, 'resource_monitor'))

                    for k, v in env.items():
                        task.specify_environment_variable(k, v)

                    for (local, remote, cache) in inputs:
                        cache_opt = wq.WORK_QUEUE_CACHE if cache else wq.WORK_QUEUE_NOCACHE
                        if os.path.isfile(local) or os.path.isdir(local):
                            task.specify_input_file(str(local), str(remote), cache_opt)
                        else:
                            logger.critical("cannot send file to worker: {0}".format(local))
                            raise NotImplementedError

                    for (local, remote) in outputs:
                        task.specify_output_file(str(local), str(remote))

                    if expiry:
                        task.specify_end_time(expiry * 10 ** 6)
                    self.queue.submit(task)

            with self.measure('status'):
                stats = self.queue.stats_hierarchy
                logger.info("{0} out of {1} workers busy; {2} tasks running, {3} waiting; {4} units left".format(
                    stats.workers_busy,
                    stats.workers_busy + stats.workers_ready,
                    stats.tasks_running,
                    stats.tasks_waiting,
                    units_left))

            with self.measure('update'):
                self.source.update(self.queue)

            # recurring actions are triggered here; plotting etc should run
            # while we have WQ hand us back tasks w/o any database
            # interaction
            with self.measure('action'):
                if action:
                    action.take()

            with self.measure('fetch'):
                starttime = time.time()
                task = self.queue.wait(interval)
                tasks = []
                while task:
                    if task.return_status == 0:
                        successful_tasks += 1
                    elif task.return_status in self.config.advanced.bad_exit_codes:
                        logger.warning(
                            "blacklisting host {0} due to bad exit code from task {1}".format(task.hostname, task.tag))
                        self.queue.blacklist(task.hostname)
                    tasks.append(task)

                    remaining = int(starttime + interval - time.time())
                    if (interval - remaining < interval_minimum or self.queue.stats.tasks_waiting > 0) and remaining > 0:
                        task = self.queue.wait(remaining)
                    else:
                        task = None
                # TODO do we really need this?  We have everything based on
                # categories by now, so this should not be needed.
                if abort_threshold > 0 and successful_tasks >= abort_threshold and not abort_active:
                    logger.info(
                        "activating fast abort with multiplier: {0}".format(abort_multiplier))
                    abort_active = True
                    self.queue.activate_fast_abort(abort_multiplier)
            if len(tasks) > 0:
                try:
                    with self.measure('return'):
                        self.source.release(tasks)
                except Exception:
                    tb = traceback.format_exc()
                    logger.critical("cannot recover from the following exception:\n" + tb)
                    util.sendemail("Your Lobster project has crashed from the following exception:\n" + tb, self.config)
                    for task in tasks:
                        logger.critical(
                            "tried to return task {0} from {1}".format(task.tag, task.hostname))
                    raise
        if units_left == 0:
            logger.info("no more work left to do")
            util.sendemail("Your Lobster project is done!", self.config)
            if self.config.elk:
                self.config.elk.end()
            if action:
                action.take(True)
