#!/usr/bin/env python
import os
import shutil
import yaml
from lobster import das_interface, sandbox, task, cmssw_config_editor, splitter
from argparse import ArgumentParser

parser = ArgumentParser(description='A job submission tool for CMS')
parser.add_argument('config_file_name', nargs='?', default='lobster.yaml', help='Configuration file to process.')
args = parser.parse_args()

with open(args.config_file_name) as config_file:
    config = yaml.load(config_file)

db = das_interface.DASInterface()

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

for config_group in config['tasks']:
    label = config_group['dataset label']
    cms_config = config_group['cmssw config']

    taskdir = os.path.join(workdir, label)
    if not os.path.exists(taskdir):
        os.makedirs(taskdir)

    dataset_info = db[config_group['dataset']]
    for id, config_params in splitter.split_by_lumi(config_group, dataset_info):
        outfile = task.insert_id(id, os.path.join(label, "output.tbz2"))
        # infile = task.insert_id(id, os.path.join(label, "output.tbz2"))
        cfgfile = task.insert_id(id, os.path.join(label, cms_config))

        cmssw_config_editor.edit_IO_files(cms_config, os.path.join(workdir, cfgfile), config_params)

        cmd = "python job.py {output} {sandbox} {input}".format(
                output=os.path.basename(outfile),
                input=os.path.basename(cfgfile),
                sandbox=sandboxfile)

        tasks.append(task.Task(id, cmd, [transform(outfile)], ['job.py', sandboxfile, transform(cfgfile)]))

with open(os.path.join(workdir, 'Makeflow'), 'w') as f:
    f.write("\n".join(map(task.Task.to_makeflow, tasks)) + "\n")
