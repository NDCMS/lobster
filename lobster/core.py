import daemon
from lockfile.pidlockfile import PIDLockFile
from lockfile import AlreadyLocked
import logging
import os
import datetime
import sys
import time
import traceback
import yaml

from lobster import job, util, cmssw

import work_queue as wq

def get_lock(workdir):
    pidfile = PIDLockFile(os.path.join(workdir, 'lobster.pid'), timeout=-1)
    try:
        pidfile.acquire()
    except AlreadyLocked:
        print "Another instance of lobster is accessing {0}".format(workdir)
        raise
    pidfile.break_lock()
    return pidfile

def kill(args):
    with open(args.configfile) as configfile:
        config = yaml.load(configfile)

    workdir = config['workdir']
    util.register_checkpoint(workdir, 'KILLED', 'PENDING')

def cleanup(args):
    with open(args.configfile) as configfile:
        config = yaml.load(configfile)

    store = cmssw.jobit.JobitStore(config)
    config = job.apply_matching(config)
    deleted_files = 0
    for cfg in config['tasks']:
        good_files = set()
        label = cfg['label']
        for id in store.finished_jobs(label):
            for base, ext in [os.path.splitext(o) for o in cfg['outputs']]:
                output_format = cfg.get("output format", "{base}_{id}.{ext}")
                good_files.add(output_format.format(base=base, id=id[0], ext=ext[1:]))

        for dirpath, dirnames, filenames in os.walk(os.path.join(config['stageout location'], label)):
            print 'Looking for output files to cleanup in {0}...'.format(label)

            files = set(filenames)
            missing = good_files - files
            extra = files - good_files

            for file in missing:
                print 'Warning!  Expected to find {0}, but it is missing!'.format(file)
            for file in extra:
                print 'Found output from failed job: deleting {0}...'.format(file)
                deleted_files += 1
                os.remove(os.path.join(dirpath, file))

    conjugation = 's' if deleted_files != 1 else ''
    print 'Finished cleaning; found {0} file{1} to clean up.'.format(deleted_files, conjugation)

def run(args):
    with open(args.configfile) as configfile:
        config = yaml.load(configfile)

    workdir = config['workdir']
    if not os.path.exists(workdir):
        os.makedirs(workdir)

    cmsjob = False
    if config.get('type', 'cmssw') == 'cmssw':
        cmsjob = True

        from ProdCommon.Credential.CredentialAPI import CredentialAPI
        cred = CredentialAPI({'credential': 'Proxy'})
        if cred.checkCredential(Time=60):
            if not 'X509_USER_PROXY' in os.environ:
                os.environ['X509_USER_PROXY'] = cred.credObj.getUserProxy()
        else:
            if config.get('check proxy', True):
                try:
                    cred.ManualRenewCredential()
                except Exception as e:
                    logging.critical("could not renew proxy")
                    sys.exit(1)
            else:
                logging.critical("please renew your proxy")
                sys.exit(1)

    mode_label = 'merge_' if args.merge else ''
    print "Saving log to {0}".format(os.path.join(workdir, mode_label+'lobster.log'))

    if not args.foreground:
        ttyfile = open(os.path.join(workdir, mode_label+'lobster.err'), 'a')
        print "Saving stderr and stdout to {0}".format(os.path.join(workdir, mode_label+'lobster.err'))

    with daemon.DaemonContext(
            detach_process=not args.foreground,
            stdout=sys.stdout if args.foreground else ttyfile,
            stderr=sys.stderr if args.foreground else ttyfile,
            working_directory=workdir,
            pidfile=get_lock(workdir)):
        logging.basicConfig(
                datefmt="%Y-%m-%d %H:%M:%S",
                format="%(asctime)s [%(levelname)s] - %(filename)s %(lineno)d: %(message)s",
                level=config.get('log level', 2) * 10,
                filename=os.path.join(workdir, mode_label+'lobster.log'))

        if args.foreground:
            console = logging.StreamHandler()
            console.setLevel(config.get('log level', 2) * 10)
            console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] - %(filename)s %(lineno)d: %(message)s"))
            logging.getLogger('').addHandler(console)

        config['configdir'] = args.configdir
        config['filepath'] = args.configfile
        config['startdir'] = args.startdir
        if args.merge:
            if args.server:
                config['stageout server'] = args.server
            config['max megabytes'] = args.max_megabytes
            job_src = cmssw.MergeProvider(config)
        elif cmsjob:
            job_src = cmssw.JobProvider(config)
        else:
            job_src = job.SimpleJobProvider(config)

        wq.cctools_debug_flags_set("all")
        wq.cctools_debug_config_file(os.path.join(workdir, mode_label+"work_queue_debug.log"))
        wq.cctools_debug_config_file_size(1 << 29)

        queue = wq.WorkQueue(-1)
        queue.specify_log(os.path.join(workdir, mode_label+"work_queue.log"))
        queue.specify_name("lobster_" + mode_label + config["id"])
        queue.specify_keepalive_timeout(300)
        # queue.tune("short-timeout", 600)
        queue.tune("transfer-outlier-factor", 4)

        logging.info("starting queue as {0}".format(queue.name))
        logging.info("submit workers with: condor_submit_workers -M {0} <num>".format(queue.name))

        payload = config.get('tune', {}).get('payload', 400)
        abort_active = False
        abort_threshold = config.get('tune', {}).get('abort threshold', 400)
        abort_multiplier = config.get('tune', {}).get('abort multiplier', 4)

        if util.checkpoint(workdir, 'KILLED') == 'PENDING':
            util.register_checkpoint(workdir, 'KILLED', 'RESTART')

        successful_jobs = 0

        creation_time = 0
        destruction_time = 0

        with open(os.path.join(workdir, mode_label+"lobster_stats.log"), "a") as statsfile:
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
                    "jobits_left\n")

        while not job_src.done():
            jobits_left = job_src.work_left()
            stats = queue.stats

            with open(os.path.join(workdir, mode_label+"lobster_stats.log"), "a") as statsfile:
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
                        jobits_left
                    ]
                    )) + "\n"
                )

            if util.checkpoint(workdir, 'KILLED') == 'PENDING':
                util.register_checkpoint(workdir, 'KILLED', str(datetime.datetime.utcnow()))
                logging.info("terminating gracefully")
                break

            logging.info("{0} out of {1} workers busy; {3} jobs running, {4} waiting; {2} jobits left".format(
                    stats.workers_busy,
                    stats.workers_busy + stats.workers_ready,
                    jobits_left,
                    stats.tasks_running,
                    stats.tasks_waiting))

            hunger = max(payload - stats.tasks_waiting, 0)

            t = time.time()
            while hunger > 0:
                jobs = job_src.obtain(50, bijective=args.bijective)

                if jobs == None or len(jobs) == 0:
                    break

                hunger -= len(jobs)

                for id, cmd, inputs, outputs in jobs:
                    task = wq.Task(cmd)
                    task.specify_tag(id)
                    task.specify_cores(1)
                    # temporary work-around?
                    # task.specify_memory(1000)
                    # task.specify_disk(4000)

                    for (local, remote) in inputs:
                        if os.path.isfile(local):
                            task.specify_input_file(str(local), str(remote), wq.WORK_QUEUE_CACHE)
                        elif os.path.isdir(local):
                            task.specify_directory(local, remote, wq.WORK_QUEUE_INPUT,
                                    wq.WORK_QUEUE_CACHE, recursive=True)
                        else:
                            logging.critical("cannot send file to worker: {0}".format(local))
                            raise NotImplementedError

                    for (local, remote) in outputs:
                        task.specify_output_file(str(local), str(remote))

                    queue.submit(task)
            creation_time += int((time.time() - t) * 1e6)

            task = queue.wait(300)
            tasks = []
            while task:
                if task.return_status == 0:
                    successful_jobs += 1
                tasks.append(task)
                if queue.stats.tasks_complete > 0:
                    task = queue.wait(1)
                else:
                    task = None
            if len(tasks) > 0:
                try:
                    t = time.time()
                    job_src.release(tasks)
                    destruction_time += int((time.time() - t) * 1e6)
                except:
                    tb = traceback.format_exc()
                    logging.critical("cannot recover from the following exception:\n" + tb)
                    for task in tasks:
                        logging.critical("tried to return task {0} from {1}".format(task.tag, task.hostname))
                    raise
            if successful_jobs >= abort_threshold and not abort_active:
                logging.info("activating fast abort with multiplier: {0}".format(abort_multiplier))
                abort_active = True
                queue.activate_fast_abort(abort_multiplier)
        if jobits_left == 0:
            logging.info("no more work left to do")
