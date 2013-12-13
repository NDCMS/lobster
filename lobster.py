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
parser.add_argument('config_file_name', nargs='?', default='lobster.yaml', help='Configuration file to process.')
args = parser.parse_args()

with open(args.config_file_name) as config_file:
    config = yaml.load(config_file)

if 'cmssw' in repr(config):
    job_src = cmssw.JobProvider(config)
else:
    job_src = job.SimpleJobProvider(config)

queue = wq.WorkQueue(-1)
queue.specify_log("wq.log")
queue.specify_name("lobster_" + config["id"])

print "Starting queue as", queue.name
print "Submit workers with: condor_submit_workers -N", queue.name, "<num>"

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

    new_jobs = 0
    while queue.hungry() and new_jobs < 150:
        new_jobs += 50
    # for i in range(queue.stats.capacity - queue.stats.workers_busy):

        t = time.time()
        jobs = job_src.obtain(50)
        print "obtain time", time.time() - t

        if len(jobs) == 0:
            break

        for id, cmd, inputs, outputs in jobs:
            task = wq.Task(cmd)
            task.specify_tag(id)

            for (local, remote) in inputs:
                if os.path.isfile(local):
                    task.specify_input_file(str(local), str(remote), wq.WORK_QUEUE_CACHE)
                elif os.path.isdir(local):
                    for (path, dirs, files) in os.walk(local):
                        for f in files:
                            lpath = os.path.join(path, f)
                            rpath = lpath.replace(local, remote)
                            task.specify_input_file(lpath, rpath, wq.WORK_QUEUE_CACHE)
                    # TODO ^^ this is a workaround for the bug in vv
                    # task.specify_directory(local, remote, wq.WORK_QUEUE_INPUT,
                            # wq.WORK_QUEUE_CACHE, recursive=True)
                else:
                    raise NotImplementedError

            for (local, remote) in outputs:
                task.specify_output_file(str(local), str(remote))

            queue.submit(task)

    print "Waiting for jobs to return..."
    task = queue.wait(3)
    while task:
        job_src.release(task.tag, task.return_status, task.output)
        if queue.stats.tasks_complete > 0:
            print "waiting..."
            task = queue.wait(3)
        else:
            task = None



