#!/usr/bin/env python
import os
from lobster.cmssw import Publisher
from argparse import ArgumentParser
import yaml

parser = ArgumentParser(description='Publish jobs.')
parser.add_argument('work_directory', help='Directory to publish.')
parser.add_argument('--clean', action='store_true', help='Remove output files for failed jobs.')
parser.add_argument('block_size', nargs='?', type=int, default=400, help='Number of files to publish per file block.')
args = parser.parse_args()

dir, label = os.path.split(os.path.normpath(args.work_directory))

with open(os.path.join(dir, 'lobster_config.yaml')) as f:
    config = yaml.load(f)

publisher = Publisher(config, dir, label)

if args.clean:
    publisher.clean()
else:
    publisher.publish(args.block_size)
