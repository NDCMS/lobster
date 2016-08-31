#!/usr/bin/env python
import argparse
import glob
import itertools
import numpy as np
import os

parser = argparse.ArgumentParser(
    description='count unique events in directories of edm files')
parser.add_argument('paths', nargs='+', help='directories to analyze')
parser.add_argument('--max', type=int, default=5000000,
                    help='max events to analyze')
parser.add_argument('--verbose', action='store_true', help='print progress')
args = parser.parse_args()

import DataFormats.FWLite

events = {}
for path in args.paths:
    files = glob.glob('{}/*.root'.format(path))

    events[path] = np.zeros(args.max, dtype='int32, int32, int32')
    for index, event in enumerate(DataFormats.FWLite.Events(files)):
        r = event.object().id().run()
        l = event.object().id().luminosityBlock()
        e = event.object().id().event()

        events[path][index] = (r, l, e)

        if args.verbose and index % 5000 == 0:
            print '>>>> entry run lumi event: {:10} {:10} {:10} {:10}'.format(index, r, l, e)

        if index == args.max:
            break

    unique = len(np.unique(events[path])) - 1

    print 'found {} ({} unique) run, lumi, event sets in {} files in path {}'.format(index + 1, unique, len(files), path)

if len(args.paths) > 1:
    for first, second in itertools.permutations(args.paths, 2):
        diff = np.setdiff1d(events[first], events[second])
        print 'path {} has {} events which are not in {}'.format(first, len(diff), second)
