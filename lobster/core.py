import daemon
from lockfile.pidlockfile import PIDLockFile
from lockfile import AlreadyLocked
import logging
import os
import sys
import yaml

from lobster import cmssw
from lobster import job

import work_queue as wq

def get_lock(workdir, kill=False):
    pidfile = PIDLockFile(os.path.join(workdir, 'lobster.pid'), timeout=-1)
    try:
        pidfile.acquire()
    except AlreadyLocked:
        if kill:
            try:
                os.kill(pidfile.read_pid(), 0)
            except OSError:
                pass

            pidfile.break_lock()

            if pidfile.is_locked():
                pidfile.release()
        else:
            print "Another instance of lobster is accessing {0}".format(workdir)
            raise
    pidfile.break_lock()
    return pidfile

def kill(args):
    with open(args.configfile) as configfile:
        config = yaml.load(configfile)

    workdir = config['workdir']

    get_lock(workdir, True)

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
        if not cred.checkCredential(Time=60):
            if config.get('check proxy', True):
                try:
                    cred.ManualRenewCredential()
                except Exception as e:
                    logging.critical("could not renew proxy")
                    sys.exit(1)
            else:
                logging.critical("please renew your proxy")
                sys.exit(1)

    print "Saving log to {0}".format(os.path.join(workdir, 'lobster.log'))

    if not args.foreground:
        ttyfile = open(os.path.join(workdir, 'lobster.err'), 'a')
        print "Saving stderr and stdout to {0}".format(os.path.join(workdir, 'lobster.err'))

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
                filename=os.path.join(workdir, 'lobster.log'))

        if args.foreground:
            console = logging.StreamHandler()
            console.setLevel(config.get('log level', 2) * 10)
            console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] - %(filename)s %(lineno)d: %(message)s"))
            logging.getLogger('').addHandler(console)

        config['configdir'] = args.configdir
        config['filepath'] = args.configfile
        config['startdir'] = args.startdir
        if cmsjob:
            job_src = cmssw.JobProvider(config)
        else:
            job_src = job.SimpleJobProvider(config)

        wq.cctools_debug_flags_set("all")
        wq.cctools_debug_config_file(os.path.join(workdir, "work_queue_debug.log"))
        wq.cctools_debug_config_file_size(1 << 29)

        queue = wq.WorkQueue(-1)
        queue.specify_log(os.path.join(workdir, "work_queue.log"))
        queue.specify_name("lobster_" + config["id"])
        queue.specify_keepalive_timeout(300)
# queue.tune("short-timeout", 600)

        logging.info("starting queue as {0}".format(queue.name))
        logging.info("submit workers with: condor_submit_workers -M {0} <num>".format(queue.name))

        payload = 400

        while not job_src.done():
            stats = queue.stats

            logging.info("{0} out of {1} workers busy; {3} jobs running, {4} waiting; {2} jobits left".format(
                    stats.workers_busy,
                    stats.workers_busy + stats.workers_ready,
                    job_src.work_left(),
                    stats.tasks_running,
                    stats.tasks_waiting))

            hunger = max(payload - stats.tasks_waiting, 0)

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
                            raise NotImplementedError

                    for (local, remote) in outputs:
                        task.specify_output_file(str(local), str(remote))

                    queue.submit(task)

            task = queue.wait(60)
            tasks = []
            while task:
                tasks.append(task)
                if queue.stats.tasks_complete > 0:
                    task = queue.wait(1)
                else:
                    task = None
            if len(tasks) > 0:
                job_src.release(tasks)
