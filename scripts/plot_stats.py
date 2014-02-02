#!/usr/bin/env python

from argparse import ArgumentParser
from collections import defaultdict
import glob
import os
import sqlite3

import matplotlib.pyplot as plt
import numpy as np

from lobster.utils import web

def diff_times(start_times, end_times):
    time_diff = lambda (a, b): (a - b).seconds / 60

    return map(lambda pair: time_diff(pair), zip(start_times, end_times))

def get_stats(dir, label):
    s = defaultdict(list)
    for subdir in glob.glob(os.path.join(dir, label, 'successful', '*')):
        (start, ready, done) = parse.get_wrapper_times(subdir)
        (finit, fopen, first, cs) = parse.get_cmssw_times(subdir)

        s['start'].append(start)
        s['ready'].append(ready)
        s['done'].append(done)
        s['finit'].append(finit)
        s['fopen'].append(fopen)
        s['first'].append(first)
        s['changed servers'].append(cs)

    return s

def make_histo(ranges, num_bins, t, xlabel, ylabel, filename, dir):
    histtype = 'bar'
    linewidth = 1
    if len(ranges) > 1:
        histtype = 'step'
        linewidth = 2

    for r, l in ranges:
        plt.hist(r, bins=num_bins, label=l, histtype=histtype)

    plt.title(t)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    if len(ranges) > 1:
        plt.legend()

    save_and_close(dir, filename)

def make_plot(tuples, x_label, y_label, t, name, dir):
    for x, y, l in tuples:
        plt.plot(x, y, label=l)
    plt.title(t)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.legend(loc='lower right')

    save_and_close(dir, name)

def make_scatter(x, x_label, y, y_label, t, name, dir):
    plt.scatter(x, y)
    plt.title(t)
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    save_and_close(dir, name)

def save_and_close(dir, name):
    print "Saving", name
    plt.savefig(os.path.join(dir, '%s.png' % name))
    plt.savefig(os.path.join(dir, '%s.pdf' % name))

    plt.close()

if __name__ == '__main__':
    parser = ArgumentParser(description='make histos')
    parser.add_argument('directory')
    parser.add_argument('wq_logfile')
    parser.add_argument('outdir')
    args = parser.parse_args()

    db = sqlite3.connect(os.path.join(args.directory, 'lobster.db'))

    stats = {}
    top_dir = args.outdir
    # top_dir = os.path.join('/afs/crc.nd.edu/user/a/awoodard/www/lobster/', '29-01-2014')
    # top_dir = os.path.join('/afs/crc.nd.edu/user/a/awoodard/www/lobster/', datetime.today().strftime('%d-%m-%Y'))

    label2id = {}
    id2label = {}

    for dset_label, dset_id in db.execute('select label, id from datasets'):
        label2id[dset_label] = dset_id
        id2label[dset_id] = dset_label

    for (label, id) in label2id.items():
        num_bins = 30
        # stats[label] = get_stats(args.directory, label)
        save_dir = os.path.join(top_dir, label)
        if not os.path.isdir(save_dir):
            os.makedirs(save_dir)

        import time
        print "Processing", label, id
        # FIXME convert dataset to id again!
        vals = np.array(db.execute('select * from jobs where status=2 and dataset=?', (label,)).fetchall(),
                dtype=[
                    ('id', 'i4'),
                    ('host', 'a50'),
                    ('dataset', 'a50'),
                    ('status', 'i4'),
                    ('exit_code', 'i4'),
                    ('missed_lumis', 'i4'),
                    ('t_submit', 'i4'),
                    ('t_send_start', 'i4'),
                    ('t_send_end', 'i4'),
                    ('t_wrapper_start', 'i4'),
                    ('t_wrapper_ready', 'i4'),
                    ('t_file_req', 'i4'),
                    ('t_file_open', 'i4'),
                    ('t_first_ev', 'i4'),
                    ('t_wrapper_end', 'i4'),
                    ('t_recv_start', 'i4'),
                    ('t_recv_end', 'i4'),
                    ('t_retrieved', 'i4'),
                    ])
        # for i in db.execute('select * from jobs where status=2 and dataset=?', (id,)).fetchall():
            # print i
        if len(vals) == 0:
            continue

        make_histo([((vals['t_wrapper_ready'] - vals['t_wrapper_start']) / 60., '')], num_bins,
                   'time from wrapper start to ready (%s)' % label,
                   'time (m)', 'jobits', 'start_to_ready', save_dir)
        make_histo([((vals['t_wrapper_end'] - vals['t_first_ev']) / 60., '')], num_bins,
                   'time from first event processed to finished (%s)' % label,
                   'time (m)', 'jobits', 'first_to_done', save_dir)
        make_histo([((vals['t_recv_end'] - vals['t_wrapper_end']) / 60., '')], num_bins,
                   'time from end of wrapper to end of transfer (%s)' % label,
                   'time (m)', 'jobits', 'end_to_over', save_dir)
        make_histo([((vals['t_recv_end'] - vals['t_recv_start']) / 60., '')], num_bins,
                   'duration of final transfer (%s)' % label,
                   'time (m)', 'jobits', 'transfer', save_dir)
        # make_histo([(diff_times(stats[label]['fopen'], stats[label]['start']), '')], num_bins,
                   # 'time from wrapper start to file open (%s)' % label,
                   # 'time (m)', 'jobits', 'start_to_fopen', save_dir)
        # make_histo([(diff_times(stats[label]['first'], stats[label]['start']), '')], num_bins,
                   # 'time from wrapper start to first event processed (%s)' % label,
                   # 'time (m)', 'jobits', 'start_to_first', save_dir)
        make_histo([((vals['t_wrapper_end'] - vals['t_wrapper_start']) / 60., '')], num_bins,
                   'time from wrapper start to processing finished (%s)' % label,
                   'time (m)', 'jobits', 'start_to_done', save_dir)
        # make_histo([(stats[label]['changed servers'], '')], num_bins,
                   # 'number of XrootD server changes (%s)' % label,
                   # 'changed_servers', 'jobits', 'server_changes', save_dir)
        # make_scatter(diff_times(stats[label]['done'], stats[label]['start']), 'time to finish processing',
                     # stats[label]['changed servers'], 'number of XrootD server changes', label, 'changed_servers_vs_time', save_dir)
        # make_scatter((vals['t_wrapper_end'] - vals['t_first_ev']) / 60., 'processing time',
                # np.char.rstrip(vals['host'], '0123456789'), 'host (cluster)', label, 'host_processing_times', save_dir)

    # make_histo([(diff_times(stats['TTJets']['done'], stats['TTJets']['first']), 'TTJets'),
                # (diff_times(stats['ttW']['done'], stats['ttW']['first']), 'ttW')], 10,
                 # 'time from first event processed to finished', 'time (m)', 'jobits', 'first_to_done_overlay', top_dir)

    print "Reading WQ log"
    with open(args.wq_logfile) as f:
        headers = dict(map(lambda (a, b): (b, a), enumerate(f.readline()[1:].split())))
    wq_stats = np.loadtxt(args.wq_logfile)
    # subtract start time, convert to minutes
    wq_stats[:,0] = (wq_stats[:,0] - wq_stats[0,1]) / 60e6
    # wq_stats = parse.get_wq_stats(args.wq_logfile)
    print "Done reading WQ log"
    run_times = map(int, wq_stats[:,0])
    make_plot([(run_times, wq_stats[:,headers['workers_active']], 'workers active'),
               (run_times, wq_stats[:,headers['workers_ready']], 'workers ready'),
               (run_times, wq_stats[:,headers['tasks_running']], 'tasks running'),
               (run_times, wq_stats[:,headers['total_workers_connected']], 'total workers connected'),
               # (run_times, wq_stats['total_workers_removed'], 'workers removed'),
               # (run_times, wq_stats['total_tasks_complete'], 'total tasks complete')
               ], 'time', 'workers active', '', 'workers_active', top_dir)

    web.update_indexes(args.outdir)
