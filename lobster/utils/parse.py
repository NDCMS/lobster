from datetime import datetime
import gzip
import os
import sys
from collections import defaultdict

def get_wq_stats(file):
    stats = defaultdict(list)
    headers = ['time', 'start time', 'workers init', 'workers ready', 'workers active', 'workers full', 'tasks waiting', 'tasks running',
               'tasks complete', 'total tasks dispatched', 'total workers joined', 'total workers connected', 'total workers removed',
               'total bytes sent', 'total bytes received', 'efficiency', 'idle percentage', 'capacity', 'average capacity', 'port', 'priority', 'total worker slots']

    with open(file) as wq_log:
        for line in wq_log.readlines():
            cols = line.split()
            try:
                int(cols[0])
            except:
                continue

            for h, v in zip(headers, cols):
                stats[h].append(v)

    return stats

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
    changed_servers = 0

    f = gzip.open(os.path.join(dir, "cmssw.log.gz"), "rb")
    for line in f.readlines():
        if "GoToAnotherServer" in line:
            changed_servers += 1
        if not finit and line[26:36] == "Initiating":
            finit = datetime.strptime(line[0:20], "%d-%b-%Y %X")
        elif not fopen and line[26:38] == "Successfully":
            fopen = datetime.strptime(line[0:20], "%d-%b-%Y %X")
        elif not first and line[21:24] == "1st":
            first = datetime.strptime(line[-29:-9], "%d-%b-%Y %X")
    f.close()
    return (finit, fopen, first, changed_servers)

