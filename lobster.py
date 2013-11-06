#!/usr/bin/env python
import os
import shutil
import yaml
from lobster import das_interface, sandbox, task, splitter, sql_interface
from argparse import ArgumentParser

parser = ArgumentParser(description='A job submission tool for CMS')
parser.add_argument('config_file_name', nargs='?', default='lobster.yaml', help='Configuration file to process.')
args = parser.parse_args()

with open(args.config_file_name) as config_file:
    config = yaml.load(config_file)

das = das_interface.DASInterface()

id = 0
tasks = []

workdir = config['workdir']

if not os.path.exists(workdir):
    os.makedirs(workdir)

shutil.copy(os.path.join(os.path.dirname(task.__file__), 'data', 'job.py'),
        os.path.join(workdir, 'job.py'))

def transform(file):
    return "{0}->{1}".format(file, os.path.basename(file))

sandboxfile = 'cmssw.tar.bz2'
sandbox.package(os.environ['LOCALRT'], os.path.join(workdir, sandboxfile))

db = sql_interface.SQLInterface(config)
db.register_jobits(das)

for config_group in config['tasks']:
    label = config_group['dataset label']
    cms_config = config_group['cmssw config']

    taskdir = os.path.join(workdir, label)
    if not os.path.exists(taskdir):
        os.makedirs(taskdir)

    shutil.copy(cms_config, taskdir)

    dataset_info = das[config_group['dataset']]
    cfgfile = os.path.join(label, cms_config)
    task_list = os.path.join(label, 'task_list.json')
    num_tasks = splitter.split_by_lumi(config_group, dataset_info, os.path.join(workdir, task_list))
    for id in range(num_tasks):
        outfile = task.insert_id(id, os.path.join(label, "output.tbz2"))

        cmd = "python job.py {output} {sandbox} {input} {task_list} {id}".format(
                output=os.path.basename(outfile),
                sandbox=sandboxfile,
                input=os.path.basename(cfgfile),
                task_list = os.path.basename(task_list),
                id=id)

        tasks.append(task.Task(id, cmd, [transform(outfile)], ['job.py', sandboxfile, transform(cfgfile), transform(task_list)]))

with open(os.path.join(workdir, 'Makeflow'), 'w') as f:
    f.write("\n".join(map(task.Task.to_makeflow, tasks)) + "\n")
