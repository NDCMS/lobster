import logging
import os
import sys
import time
import yaml

from lobster import cmssw
from lobster import job

import work_queue as wq

def run(args):
    with open(args.configfile) as configfile:
        config = yaml.load(configfile)

    logging.basicConfig(
            datefmt="%Y-%m-%d %H:%M:%S",
            format="%(asctime)s [%(levelname)s] - %(filename)s %(lineno)d: %(message)s",
            level=config.get('log level', 2) * 10)

    config['filepath'] = args.configfile
    if config.get('type', 'cmssw') == 'simple':
        job_src = job.SimpleJobProvider(config)
    else:
        if config.get('check proxy', True):
            from ProdCommon.Credential.CredentialAPI import CredentialAPI
            cred = CredentialAPI({'credential': 'Proxy'})
            if not cred.checkCredential(Time=60):
                try:
                    cred.ManualRenewCredential()
                except Exception as e:
                    print e
                    sys.exit(1)

        job_src = cmssw.JobProvider(config)

    wq.cctools_debug_flags_set("all")
    wq.cctools_debug_config_file(os.path.join(config["workdir"], "debug.log"))
    wq.cctools_debug_config_file_size(1 << 29)

    queue = wq.WorkQueue(-1)
    queue.specify_log(os.path.join(config["workdir"], "work_queue.log"))
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
            t = time.time()
            jobs = job_src.obtain(50, bijective=args.bijective)
            with open(os.path.join(config["workdir"], 'debug_lobster_times'), 'a') as f:
                delta = time.time() - t
                if jobs == None:
                    size = 0
                else:
                    size = len(jobs)
                ratio = delta / float(size) if size != 0 else 0
                f.write("CREA {0} {1} {2}\n".format(size, delta, ratio))

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

        task = queue.wait(3)
        tasks = []
        while task:
            tasks.append(task)
            if queue.stats.tasks_complete > 0:
                task = queue.wait(1)
            else:
                task = None
        if len(tasks) > 0:
            t = time.time()
            job_src.release(tasks)
            with open(os.path.join(config["workdir"], 'debug_lobster_times'), 'a') as f:
                delta = time.time() - t
                size = len(tasks)
                ratio = delta / float(size) if size != 0 else 0
                f.write("RECV {0} {1} {2}\n".format(size, delta, ratio))

if __name__ == '__main__':
    main()
