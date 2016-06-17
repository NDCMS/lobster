from contextlib import contextmanager

import daemon
import datetime
import inspect
import logging
import logging.handlers
import os
import resource
import signal
import sys
import time
import traceback

from lobster import actions, util
from lobster.commands.status import Status
from lobster.core.command import Command
from lobster.core.source import TaskProvider

from pkg_resources import get_distribution

import work_queue as wq

logger = logging.getLogger('lobster.core')

class Terminate(Command):
    @property
    def help(self):
        return 'terminate running lobster instance'

    def setup(self, argparser):
        pass

    def run(self, args):
        logger.info("setting flag to quit at the next checkpoint")
        logger.debug("Don't be alarmed.  The following stack trace doesn't indicate a crash.  It's just for debugging purposes.")
        logger.debug("stack:\n{0}".format(''.join(traceback.format_stack())))
        workdir = args.config.workdir
        util.register_checkpoint(workdir, 'KILLED', 'PENDING')

class Process(Command):
    def __init__(self):
        self.times = {
                'action': 0,
                'create': 0,
                'fetch': 0,
                'return': 0,
                'status': 0,
                'update': 0
        }

    @contextmanager
    def measure(self, what):
        t = time.time()
        yield
        self.times[what] += int((time.time() - t) * 1e6)

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
                [getattr(stats, a) for a in self.log_attributes]
                )) + "\n"
            )

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
            util.register_checkpoint(self.config.workdir, "version", get_distribution('Lobster').version)
        else:
            util.verify(self.config.workdir)

        from WMCore.Credential.Proxy import Proxy
        cred = Proxy({'logger': logging.getLogger("WMCore"), 'proxyValidity': '192:00'})
        if cred.check() and cred.getTimeLeft() > 4 * 3600:
            if not 'X509_USER_PROXY' in os.environ:
                os.environ['X509_USER_PROXY'] = cred.getProxyFilename()
        else:
            if self.config.advanced.renew_proxy:
                cred.renew()
                if cred.getTimeLeft() < 4 * 3600:
                    logger.error("could not renew proxy")
                    sys.exit(1)
                os.environ['X509_USER_PROXY'] = cred.getProxyFilename()
            else:
                logger.error("please renew your proxy")
                sys.exit(1)

        if not args.foreground:
            ttyfile = open(os.path.join(self.config.workdir, 'process.err'), 'a')
            logger.info("saving stderr and stdout to {0}".format(os.path.join(self.config.workdir, 'process.err')))

        if self.config.advanced.dump_core:
            logger.info("setting core dump size to unlimited")
            resource.setrlimit(resource.RLIMIT_CORE, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

        signals = daemon.daemon.make_default_signal_map()
        signals[signal.SIGINT] = lambda num, frame: Terminate().run(args)
        signals[signal.SIGTERM] = lambda num, frame: Terminate().run(args)

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
                logger.info("stderr and stdout saved in {0}".format(os.path.join(self.config.workdir, 'process.err')))

            try:
                # Fails if something with working directory creation went wrong
                Status().run(args)
            except:
                pass

    def sprint(self):
        with util.PartiallyMutable.unlock():
            task_src = TaskProvider(self.config)
        action = actions.Actions(self.config, task_src)
        from WMCore.Credential.Proxy import Proxy
        proxy = Proxy({'logger': logging.getLogger("WMCore")})

        logger.info("using wq from {0}".format(wq.__file__))

        wq.cctools_debug_flags_set("all")
        wq.cctools_debug_config_file(os.path.join(self.config.workdir, "work_queue_debug.log"))
        wq.cctools_debug_config_file_size(1 << 29)

        self.queue = wq.WorkQueue(-1)
        self.queue.specify_log(os.path.join(self.config.workdir, "work_queue.log"))
        self.queue.specify_transactions_log(os.path.join(self.config.workdir, "transactions.log"))
        self.queue.specify_name("lobster_" + self.config.label)
        self.queue.specify_keepalive_timeout(300)
        # self.queue.tune("short-timeout", 600)
        self.queue.tune("transfer-outlier-factor", 4)
        self.queue.specify_algorithm(wq.WORK_QUEUE_SCHEDULE_RAND)
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

        bad_exitcodes = self.config.advanced.bad_exit_codes
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

        while not task_src.done():
            with self.measure('status'):
                tasks_left = task_src.tasks_left()
                units_left = task_src.work_left()

                logger.debug("expecting {0} tasks, still".format(tasks_left))
                self.queue.specify_num_tasks_left(tasks_left)

                for c in categories + ['all']:
                    self.log(c, units_left)

                if util.checkpoint(self.config.workdir, 'KILLED') == 'PENDING':
                    util.register_checkpoint(self.config.workdir, 'KILLED', str(datetime.datetime.utcnow()))

                    # let the task source shut down gracefully
                    logger.info("terminating task source")
                    task_src.terminate()
                    logger.info("terminating gracefully")
                    break

            with self.measure('action'):
                expiry = None
                if proxy:
                    left = proxy.getTimeLeft()
                    if left == 0:
                        logger.error("proxy expired!")
                        task_src.terminate()
                        break
                    elif left < 4 * 3600:
                        logger.warn("only {0}:{1:02} left in proxy lifetime!".format(left / 3600, left / 60))
                    expiry = int(time.time()) + left

            with self.measure('create'):
                have = {}
                for c in categories:
                    cstats = self.queue.stats_category(c)
                    have[c] = {'running': cstats.tasks_running, 'queued': cstats.tasks_waiting}

                stats = self.queue.stats_hierarchy
                tasks = task_src.obtain(stats.total_cores, have)

                for category, cmd, id, inputs, outputs, env, dir in tasks:
                    task = wq.Task(cmd)
                    task.specify_category(category)
                    task.specify_tag(id)
                    task.specify_max_retries(wq_max_retries)
                    task.specify_monitor_output(os.path.join(dir, 'resource_monitor'))

                    for k, v in env.items():
                        task.specify_environment_variable(k, v)

                    for (local, remote, cache) in inputs:
                        if os.path.isfile(local):
                            cache_opt = wq.WORK_QUEUE_CACHE if cache else wq.WORK_QUEUE_NOCACHE
                            task.specify_input_file(str(local), str(remote), cache_opt)
                        elif os.path.isdir(local):
                            task.specify_directory(str(local), str(remote), wq.WORK_QUEUE_INPUT,
                                    wq.WORK_QUEUE_CACHE, recursive=True)
                        else:
                            logger.critical("cannot send file to worker: {0}".format(local))
                            raise NotImplementedError

                    for (local, remote) in outputs:
                        task.specify_output_file(str(local), str(remote))

                    if expiry:
                        task.specify_end_time(expiry * 10**6)
                    self.queue.submit(task)

            with self.measure('status'):
                stats = self.queue.stats_hierarchy
                logger.info("{0} out of {1} workers busy; {3} tasks running, {4} waiting; {2} units left".format(
                        stats.workers_busy,
                        stats.workers_busy + stats.workers_ready,
                        units_left,
                        stats.tasks_running,
                        stats.tasks_waiting))

            with self.measure('update'):
                task_src.update(self.queue)

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
                    elif task.return_status in bad_exitcodes:
                        logger.warning("blacklisting host {0} due to bad exit code from task {1}".format(task.hostname, task.tag))
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
                    logger.info("activating fast abort with multiplier: {0}".format(abort_multiplier))
                    abort_active = True
                    self.queue.activate_fast_abort(abort_multiplier)
            if len(tasks) > 0:
                try:
                    with self.measure('return'):
                        task_src.release(tasks)
                except:
                    tb = traceback.format_exc()
                    logger.critical("cannot recover from the following exception:\n" + tb)
                    for task in tasks:
                        logger.critical("tried to return task {0} from {1}".format(task.tag, task.hostname))
                    raise
        if units_left == 0:
            logger.info("no more work left to do")
            if action:
                action.take(True)
