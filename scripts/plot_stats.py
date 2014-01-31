#!/usr/bin/env python

from argparse import ArgumentParser
from collections import defaultdict
import glob
import os

import matplotlib.pyplot as plt

from lobster.utils import parse, web

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
    plt.savefig(os.path.join(dir, '%s.png' % name))
    plt.savefig(os.path.join(dir, '%s.pdf' % name))

    plt.close()

if __name__ == '__main__':
    parser = ArgumentParser(description='make histos')
    parser.add_argument('directory')
    parser.add_argument('wq_logfile')
    parser.add_argument('outdir')
    args = parser.parse_args()

    stats = {}
    top_dir = args.outdir
    # top_dir = os.path.join('/afs/crc.nd.edu/user/a/awoodard/www/lobster/', '29-01-2014')
    # top_dir = os.path.join('/afs/crc.nd.edu/user/a/awoodard/www/lobster/', datetime.today().strftime('%d-%m-%Y'))

    for label, num_bins in [('TTJets', 15), ('ttW', 7)]:
        stats[label] = get_stats(args.directory, label)
        save_dir = os.path.join(top_dir, label)
        if not os.path.isdir(save_dir):
            os.makedirs(save_dir)

        make_histo([(diff_times(stats[label]['ready'], stats[label]['start']), '')], num_bins,
                   'time from wrapper start to ready (%s)' % label,
                   'time (m)', 'jobits', 'start_to_ready', save_dir)
        make_histo([(diff_times(stats[label]['done'], stats[label]['first']), '')], num_bins,
                   'time from first event processed to finished (%s)' % label,
                   'time (m)', 'jobits', 'first_to_done', save_dir)
        make_histo([(diff_times(stats[label]['fopen'], stats[label]['start']), '')], num_bins,
                   'time from wrapper start to file open (%s)' % label,
                   'time (m)', 'jobits', 'start_to_fopen', save_dir)
        make_histo([(diff_times(stats[label]['first'], stats[label]['start']), '')], num_bins,
                   'time from wrapper start to first event processed (%s)' % label,
                   'time (m)', 'jobits', 'start_to_first', save_dir)
        make_histo([(diff_times(stats[label]['done'], stats[label]['start']), '')], num_bins,
                   'time from wrapper start to processing finished (%s)' % label,
                   'time (m)', 'jobits', 'start_to_done', save_dir)
        make_histo([(stats[label]['changed servers'], '')], num_bins,
                   'number of XrootD server changes (%s)' % label,
                   'changed_servers', 'jobits', 'server_changes', save_dir)
        make_scatter(diff_times(stats[label]['done'], stats[label]['start']), 'time to finish processing',
                     stats[label]['changed servers'], 'number of XrootD server changes', label, 'changed_servers_vs_time', save_dir)

    make_histo([(diff_times(stats['TTJets']['done'], stats['TTJets']['first']), 'TTJets'),
                (diff_times(stats['ttW']['done'], stats['ttW']['first']), 'ttW')], 10,
                 'time from first event processed to finished', 'time (m)', 'jobits', 'first_to_done_overlay', top_dir)

    wq_stats = parse.get_wq_stats(args.wq_logfile)
    run_times = [(int(x) - int(y)) / (60 * 1e6) for x, y in zip(wq_stats['time'], wq_stats['start time'])]
    make_plot([(run_times, wq_stats['workers active'], 'workers active'),
               (run_times, wq_stats['workers ready'], 'workers ready'),
               (run_times, wq_stats['tasks running'], 'tasks running'),
               (run_times, wq_stats['total workers connected'], 'total workers connected'),
               (run_times, wq_stats['total workers connected'], 'total tasks complete')], 'time', 'workers active', '', 'workers_active', top_dir)

    web.update_indexes(args.outdir)
