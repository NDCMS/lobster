from datetime import datetime
import gzip
import os
import sys

def get_wrapper_times(dir):
    start = None
    ready = None
    done = None

    with open(os.path.join(dir, "job.log")) as f:
        for line in f.readlines():
            if not line.startswith('[') or line[20] != ']':
                continue
            if line.endswith('start\n'):
                start = datetime.strptime(line[1:20], "%Y-%m-%d %X")
            elif line.endswith('ready\n'):
                ready = datetime.strptime(line[1:20], "%Y-%m-%d %X")
            elif line.endswith('done\n'):
                done = datetime.strptime(line[1:20], "%Y-%m-%d %X")
    return (start, ready, done)

def get_cmssw_times(dir):
    finit = None
    fopen = None
    first = None

    f = gzip.open(os.path.join(dir, "cmssw.log.gz"), "rb")
    for line in f.readlines():
        if not finit and line[26:36] == "Initiating":
            finit = datetime.strptime(line[0:20], "%d-%b-%Y %X")
        elif not fopen and line[26:38] == "Successfully":
            fopen = datetime.strptime(line[0:20], "%d-%b-%Y %X")
        elif not first and line[21:24] == "1st":
            first = datetime.strptime(line[-29:-9], "%d-%b-%Y %X")
    f.close()
    return (finit, fopen, first)

