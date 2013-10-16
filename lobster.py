#!/usr/bin/env python
import os
import yaml
from lobster import das_interface, sandbox
from argparse import ArgumentParser

parser = ArgumentParser(description='A job submission tool for CMS')
parser.add_argument('config_file_name', nargs='?', default='lobster.yaml', help='Configuration file to process.')
args = parser.parse_args()

with open(args.config_file_name) as config_file:
    config = yaml.load(config_file)

sandbox.package(os.environ['LOCALRT'], 'cmssw.tar.bz2')
