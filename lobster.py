#!/usr/bin/env python
import os
import shutil
import time
import yaml
from lobster import cmssw
from lobster import job
from argparse import ArgumentParser

import work_queue as wq

parser = ArgumentParser(description='A job submission tool for CMS')
parser.add_argument('config_file_name', nargs='?', default='test/lobster.yaml', help='Configuration file to process.')
parser.add_argument('--bijective', '-i', action='store_true', default=False, help='Use a 1-1 mapping for input and output files (process one input file per output file).')
args = parser.parse_args()

with open(args.config_file_name) as config_file:
    config = yaml.load(config_file)

config['filepath'] = args.config_file_name
if 'cmssw' in repr(config):
    job_src = cmssw.JobProvider(config)
else:
    job_src = job.SimpleJobProvider(config)

wq.cctools_debug_flags_set("all")
wq.cctools_debug_config_file(os.path.join(config["workdir"], "debug.log"))
wq.cctools_debug_config_file_size(1 << 29)

queue = wq.WorkQueue(-1)
queue.specify_log(os.path.join(config["workdir"], "work_queue.log"))
queue.specify_name("lobster_" + config["id"])
queue.specify_keepalive_timeout(300)
# queue.tune("short-timeout", 600)

print "Starting queue as", queue.name
print "Submit workers with: condor_submit_workers -N", queue.name, "<num>"

payload = 400

while not job_src.done():
    stats = queue.stats

    print "Status: Slaves {0}/{1} - Jobs {3}/{4}/{5} - Work {2} [{6}]".format(
            stats.workers_busy,
            stats.workers_busy + stats.workers_ready,
            job_src.work_left(),
            stats.tasks_waiting,
            stats.tasks_running,
            stats.tasks_complete,
            time.strftime("%d %b %Y %H:%M:%S", time.localtime()))

    hunger = max(payload - stats.tasks_waiting, 0)

    while hunger > 0:
        t = time.time()
        jobs = job_src.obtain(50, bijective=args.bijective)
        with open('debug_lobster_times', 'a') as f:
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
            task.specify_memory(1100)
            task.specify_disk(4000)

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

    print "Waiting for jobs to return..."
    task = queue.wait(3)
    tasks = []
    while task:
        tasks.append(task)
        if queue.stats.tasks_complete > 0:
            task = queue.wait(1)
        else:
            task = None
    print "Done waiting..."
    if len(tasks) > 0:
        t = time.time()
        job_src.release(tasks)
        with open('debug_lobster_times', 'a') as f:
            delta = time.time() - t
            size = len(tasks)
            ratio = delta / float(size) if size != 0 else 0
            f.write("RECV {0} {1} {2}\n".format(size, delta, ratio))
