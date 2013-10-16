#!/usr/bin/env python
import os
import yaml
from lobster import das_interface, sandbox, cmssw_config_editor
from argparse import ArgumentParser

parser = ArgumentParser(description='A job submission tool for CMS')
parser.add_argument('config_file_name', nargs='?', default='lobster.yaml', help='Configuration file to process.')
args = parser.parse_args()

with open(args.config_file_name) as config_file:
    config = yaml.load(config_file)

sandbox.package(os.environ['LOCALRT'], 'cmssw.tar.bz2')

for config_group in config:
    das_interface = das_interface.DASInterface()
    dataset_files = das_interface.get_files(config_group['dataset'])

    cmssw_config_editor.edit_IO_files(config_group['dataset'], config_group['dataset label'], dataset_files, config_group['config'])

