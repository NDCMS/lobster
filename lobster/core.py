import daemon
import datetime
import logging
import logging.handlers
import multiprocessing
import os
import resource
import signal
import sys
import threading
import time
import traceback
import yaml

from lobster import cmssw, job, util

from pkg_resources import get_distribution

import work_queue as wq

logger = multiprocessing.get_logger()


class ShortPathFormatter(logging.Formatter):
    def format(self, record):
        if len(record.pathname) > 40:
            record.pathname = '...' + record.pathname[-37:]
        # FIXME at some point, Formatter is a new-style class and we can
        # use super
        # return super(ShortPathFormatter, self).format(record)
        return logging.Formatter.format(self, record)


def kill(args):
    logger.info("setting flag to quit at the next checkpoint")
    with open(args.configfile) as configfile:
        config = yaml.load(configfile)

    workdir = config['workdir']
    util.register_checkpoint(workdir, 'KILLED', 'PENDING')

def run(args):
    with open(args.configfile) as configfile:
        config = yaml.load(configfile)

    workdir = config['workdir']
    if not os.path.exists(workdir):
        os.makedirs(workdir)
        util.register_checkpoint(workdir, "version", get_distribution('Lobster').version)
    else:
        util.verify(workdir)

    cmsjob = False
    if config.get('type', 'cmssw') == 'cmssw':
        cmsjob = True

        from ProdCommon.Credential.CredentialAPI import CredentialAPI
        cred = CredentialAPI({'credential': 'Proxy'})
        if cred.checkCredential(Time=60):
            if not 'X509_USER_PROXY' in os.environ:
                os.environ['X509_USER_PROXY'] = cred.credObj.getUserProxy()
        else:
            if config.get('advanced', {}).get('renew proxy', True):
                try:
                    cred.ManualRenewCredential()
                except Exception as e:
                    print("could not renew proxy")
                    sys.exit(1)
            else:
                print("please renew your proxy")
                sys.exit(1)

    print "Saving log to {0}".format(os.path.join(workdir, 'lobster.log'))

    if not args.foreground:
        ttyfile = open(os.path.join(workdir, 'lobster.err'), 'a')
        print "Saving stderr and stdout to {0}".format(os.path.join(workdir, 'lobster.err'))

    if config.get('advanced', {}).get('dump core', False):
        print "Setting core dump size to unlimited"
        resource.setrlimit(resource.RLIMIT_CORE, (resource.RLIM_INFINITY, resource.RLIM_INFINITY))

    signals = daemon.daemon.make_default_signal_map()
    signals[signal.SIGTERM] = lambda num, frame: kill(args)

    with daemon.DaemonContext(
            detach_process=not args.foreground,
            stdout=sys.stdout if args.foreground else ttyfile,
            stderr=sys.stderr if args.foreground else ttyfile,
            working_directory=workdir,
            pidfile=util.get_lock(workdir, args.force),
            prevent_core=False,
            signal_map=signals):

        level = max(1, config.get('advanced', {}).get('log level', 2) + args.quiet - args.verbose) * 10
        fileh = logging.handlers.RotatingFileHandler(os.path.join(workdir, 'lobster.log'), maxBytes=500e6, backupCount=10)
        fileh.setFormatter(ShortPathFormatter("%(asctime)s [%(levelname)5s] - %(pathname)-40s %(lineno)4d: %(message)s"))
        fileh.setLevel(level)

        logger.addHandler(fileh)
        logger.setLevel(level)

        if args.foreground:
            console = logging.StreamHandler()
            console.setLevel(level)
            console.setFormatter(ShortPathFormatter("%(asctime)s [%(levelname)5s] - %(pathname)-40s %(lineno)4d: %(message)s"))
            logger.addHandler(console)

        config['base directory'] = args.configdir
        config['base configuration'] = args.configfile
        config['startup directory'] = args.startdir

        t = threading.Thread(target=sprint, args=(config, workdir, cmsjob))
        t.start()
        t.join()

        logger.info("lobster terminated")

def sprint(config, workdir, cmsjob):
    if cmsjob:
        job_src = cmssw.JobProvider(config)
        actions = cmssw.Actions(config)
    else:
        job_src = job.SimpleJobProvider(config)
        actions = None

    logger.info("using wq from {0}".format(wq.__file__))

    wq.cctools_debug_flags_set("all")
    wq.cctools_debug_config_file(os.path.join(workdir, "work_queue_debug.log"))
    wq.cctools_debug_config_file_size(1 << 29)

    queue = wq.WorkQueue(-1)
    queue.specify_log(os.path.join(workdir, "work_queue.log"))
    queue.specify_name("lobster_" + config["id"])
    queue.specify_keepalive_timeout(300)
    # queue.tune("short-timeout", 600)
    queue.tune("transfer-outlier-factor", 4)
    queue.specify_algorithm(wq.WORK_QUEUE_SCHEDULE_RAND)


    cores = config.get('cores per job', 1)
    logger.info("starting queue as {0}".format(queue.name))
    logger.info("submit workers with: condor_submit_workers -M {0}{1} <num>".format(
        queue.name, ' --cores {0}'.format(cores) if cores > 1 else ''))

    payload = config.get('advanced', {}).get('payload', 10)
    abort_active = False
    abort_threshold = config.get('advanced', {}).get('abort threshold', 400)
    abort_multiplier = config.get('advanced', {}).get('abort multiplier', 4)

    if util.checkpoint(workdir, 'KILLED') == 'PENDING':
        util.register_checkpoint(workdir, 'KILLED', 'RESTART')

    # time in seconds to wait for WQ to return tasks, with minimum wait
    # time in case no more tasks are waiting
    interval = 60
    interval_minimum = 10

    jobits_left = 0
    successful_jobs = 0

    creation_time = 0
    destruction_time = 0

    with open(os.path.join(workdir, "lobster_stats.log"), "a") as statsfile:
        statsfile.write(
                "#timestamp " +
                "total_workers_connected total_workers_joined total_workers_removed " +
                "workers_busy workers_idle " +
                "tasks_running " +
                "total_send_time total_receive_time " +
                "total_create_time total_return_time " +
                "idle_percentage " +
                "capacity " +
                "efficiency " +
                "total_memory " +
                "total_cores " +
                "jobits_left\n")

    bad_exitcodes = job_src.bad_exitcodes

    while not job_src.done():
        jobits_left = job_src.work_left()
        stats = queue.stats_hierarchy

        with open(os.path.join(workdir, "lobster_stats.log"), "a") as statsfile:
            now = datetime.datetime.now()
            statsfile.write(" ".join(map(str,
                [
                    int(int(now.strftime('%s')) * 1e6 + now.microsecond),
                    stats.total_workers_connected,
                    stats.total_workers_joined,
                    stats.total_workers_removed,
                    stats.workers_busy,
                    stats.workers_idle,
                    stats.tasks_running,
                    stats.total_send_time,
                    stats.total_receive_time,
                    creation_time,
                    destruction_time,
                    stats.idle_percentage,
                    stats.capacity,
                    stats.efficiency,
                    stats.total_memory,
                    stats.total_cores,
                    jobits_left
                ]
                )) + "\n"
            )

        if util.checkpoint(workdir, 'KILLED') == 'PENDING':
            util.register_checkpoint(workdir, 'KILLED', str(datetime.datetime.utcnow()))

            # let the job source shut down gracefully
            logger.info("terminating job source")
            job_src.terminate()
            logger.info("terminating gracefully")
            break

        logger.info("{0} out of {1} workers busy; {3} jobs running, {4} waiting; {2} jobits left".format(
                stats.workers_busy,
                stats.workers_busy + stats.workers_ready,
                jobits_left,
                stats.tasks_running,
                stats.tasks_waiting))

        # FIXME switch to resource monitoring in WQ
        need = max(payload, stats.total_cores / 10) + stats.total_cores - stats.committed_cores
        hunger = max(need - stats.tasks_waiting, 0)

        logger.debug("total cores available (committed): {0} ({1})".format(stats.total_cores, stats.committed_cores))
        logger.debug("trying to feed {0} jobs to work queue".format(hunger))

        t = time.time()
        while hunger > 0:
            jobs = job_src.obtain(hunger)

            if jobs == None or len(jobs) == 0:
                break

            hunger -= len(jobs)
            for cores, cmd, id, inputs, outputs in jobs:
                task = wq.Task(cmd)
                task.specify_tag(id)
                task.specify_cores(cores)
                # temporary work-around?
                # task.specify_memory(1000)
                # task.specify_disk(4000)

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

                queue.submit(task)
        creation_time += int((time.time() - t) * 1e6)

        job_src.update(queue)
        starttime = time.time()
        task = queue.wait(interval)
        tasks = []
        while task:
            if task.return_status == 0:
                successful_jobs += 1
            elif task.return_status in bad_exitcodes:
                logger.warning("blacklisting host {0} due to bad exit code from job {1}".format(task.hostname, task.tag))
                queue.blacklist(task.hostname)
            tasks.append(task)

            remaining = int(starttime + interval - time.time())
            if (interval - remaining > interval_minimum or queue.stats.tasks_waiting > 0) and remaining > 0:
                task = queue.wait(remaining)
            else:
                task = None
        if len(tasks) > 0:
            try:
                t = time.time()
                job_src.release(tasks)
                destruction_time += int((time.time() - t) * 1e6)
            except:
                tb = traceback.format_exc()
                logger.critical("cannot recover from the following exception:\n" + tb)
                for task in tasks:
                    logger.critical("tried to return task {0} from {1}".format(task.tag, task.hostname))
                raise
        if abort_threshold > 0 and successful_jobs >= abort_threshold and not abort_active:
            logger.info("activating fast abort with multiplier: {0}".format(abort_multiplier))
            abort_active = True
            queue.activate_fast_abort(abort_multiplier)

        # recurring actions are triggered here
        if actions:
            actions.take()
    if jobits_left == 0:
        logger.info("no more work left to do")
