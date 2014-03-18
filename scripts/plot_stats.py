#!/usr/bin/env python

from argparse import ArgumentParser
from os.path import expanduser
from collections import defaultdict
import glob
import math
import os, sys
import sqlite3

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

matplotlib.rc('axes', labelsize='large')
matplotlib.rc('figure', figsize=(8, 1.5))
matplotlib.rc('figure.subplot', left=0.09, right=0.92, bottom=0.22)
matplotlib.rc('font', size=7)

class SmartList(list):
    """Stupid extended list."""
    def __init__(self, *args, **kwargs):
        list.__init__(self, *args, **kwargs)
    def __add__(self, other):
        return super(SmartList, self).__add__([other])
    def __iadd__(self, other):
        return super(SmartList, self).__iadd__([other])

def html_tag(tag, *args, **kwargs):
    attr = " ".join(['{0}="{1}"'.format(a, b.replace('"', r'\"')) for a, b in kwargs.items()])
    return '<{0}>\n{1}\n</{2}>\n'.format(" ".join([tag, attr]), "\n".join(args), tag)

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

def make_histo(a, num_bins, xlabel, ylabel, filename, dir, **kwargs):
    # fig, (ax0, ax1) = plt.subplots(nrows=2, sharex=True)
    if 'log' in kwargs:
        if kwargs['log']:
            plt.yscale('log')
        del kwargs['log']

    plt.hist(a, bins=num_bins, histtype='barstacked', **kwargs)

    # ax0.set_title(t)

    # ax0.set_ylabel(ylabel)
    # ax1.set_xlabel(xlabel)
    # ax1.set_ylabel(ylabel)

    # plt.set_title(t)
    # plt.set_xlabel(xlabel)
    # plt.set_ylabel(ylabel)

    # plt.title(t)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)

    if 'label' in kwargs:
        plt.legend()

    return save_and_close(dir, filename)

def make_plot(tuples, x_label, y_label, name, dir, fun=matplotlib.axes.Axes.plot, y_label2=None, xmin=0, xmax=None):
    fig, ax1 = plt.subplots()

    plots1 = tuples[0] if y_label2 else tuples
    for x, y, l in plots1:
        fun(ax1, x, y, label=l)
    ax1.set_xlabel(x_label)
    ax1.set_xlim(left=xmin)
    if xmax is not None:
        ax1.set_xlim(right=int(xmax))
    ax1.set_ylabel(y_label)
    ax1.legend(loc='upper left')

    if y_label2:
        ax2 = ax1.twinx()
        for x, y, l in tuples[1]:
            fun(ax2, x, y, ':', label=l)
        ax2.set_ylabel(y_label2)
        ax2.set_xlim(left=xmin)
        if xmax is not None:
            ax2.set_xlim(right=int(xmax))
        ax2.legend(loc='upper right')

    # plt.legend()
    num = len(tuples[0]) + len(tuples[1]) if y_label2 else len(tuples)

    return save_and_close(dir, name)

def make_scatter(x, x_label, y, y_label, t, name, dir):
    plt.scatter(x, y)
    plt.title(t)
    plt.xlabel(x_label)
    plt.ylabel(y_label)

    return save_and_close(dir, name)

def reduce(a, idx, interval):
    quant = a[:,idx]
    last = quant[0]
    select = np.ones((len(quant),), dtype=np.bool)
    for i in range(1, len(quant) - 1):
        if quant[i] - last > interval or quant[i + 1] - last > interval:
            select[i] = True
            last = quant[i]
        else:
            select[i] = False

    return a[select]

def split_by_column(a, col, key=lambda x: x):
    """Split an array into multiple ones, based on unique values in the named
    column `col`.
    """
    vals = np.unique(a[col])
    ret = []
    for v in vals:
        sel = a[col] == v
        ret.append((key(v), a[sel]))
    return ret

def save_and_close(dir, name):
    if not os.path.exists(dir):
        os.makedirs(dir)
    print "Saving", name
    # plt.gcf().set_size_inches(6, 1.5)
    plt.savefig(os.path.join(dir, '%s.png' % name))
    plt.savefig(os.path.join(dir, '%s.pdf' % name))

    plt.close()

    return html_tag("img", src='{0}.png'.format(name))

if __name__ == '__main__':
    parser = ArgumentParser(description='make histos')
    parser.add_argument('directory', help="Specify input directory")
    parser.add_argument('outdir', nargs='*', help="Specify output directory")
    parser.add_argument("--xmin", type=int, help="Specify custom x-axis minimum", default=0)
    parser.add_argument("--xmax", help="Specify custom x-axis maximum")
    args = parser.parse_args()

    db = sqlite3.connect(os.path.join(args.directory, 'lobster.db'))
    stats = {}

    if len(args.outdir) is not 0:
        top_dir = args.outdir[0]
    else:
        top_dir = args.directory
        if len(top_dir.split("/")[-1]) > 0:
           top_dir = top_dir.split("/")[-1]
        else:
           top_dir = top_dir.split("/")[-2]

        www_dir = expanduser("~") + '/www/'
        if not os.path.isdir(www_dir):
            raise IOError("Web output directory '" + www_dir + "' does not exist or is not accessible.")
        top_dir = www_dir + top_dir

    # top_dir = os.path.join('/afs/crc.nd.edu/user/a/awoodard/www/lobster/', '29-01-2014')
    # top_dir = os.path.join('/afs/crc.nd.edu/user/a/awoodard/www/lobster/', datetime.today().strftime('%d-%m-%Y'))
    print 'Saving plots to: ' + top_dir

    failed_jobs = np.array(db.execute("""select
        id,
        host,
        dataset,
        exit_code,
        time_submit,
        time_retrieved
        from jobs where status=3""").fetchall(),
            dtype=[
                ('id', 'i4'),
                ('host', 'a50'),
                ('dataset', 'i4'),
                ('exit_code', 'i4'),
                ('t_submit', 'i4'),
                ('t_retrieved', 'i4')
                ])

    success_jobs = np.array(db.execute('select * from jobs where status=2').fetchall(),
            dtype=[
                ('id', 'i4'),
                ('host', 'a50'),
                ('dataset', 'i4'),
                ('status', 'i4'),
                ('exit_code', 'i4'),
                ('published_file_block', 'a50'),
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
                ('t_retrieved', 'i4')
                ])

    label2id = {}
    id2label = {}

    for dset_label, dset_id in db.execute('select label, id from datasets'):
        label2id[dset_label] = dset_id
        id2label[dset_id] = dset_label

    dset_values = split_by_column(success_jobs, 'dataset', key=lambda x: id2label[x])

    num_bins = 30
    total_times = [(vs[1]['t_wrapper_end'] - vs[1]['t_wrapper_start']) / 60. for vs in dset_values]
    processing_times = [(vs[1]['t_wrapper_end'] - vs[1]['t_first_ev']) / 60. for vs in dset_values]
    overhead_times = [(vs[1]['t_first_ev'] - vs[1]['t_wrapper_start']) / 60. for vs in dset_values]
    stageout_times = [(vs[1]['t_retrieved'] - vs[1]['t_wrapper_end']) / 60. for vs in dset_values]

    jtags = SmartList()
    wtags = SmartList()

    jtags += make_histo(total_times, num_bins, 'Runtime (m)', 'Jobs', 'run_time', top_dir, label=[vs[0] for vs in dset_values])
    jtags += make_histo(processing_times, num_bins, 'Pure processing time (m)', 'Jobs', 'processing_time', top_dir, label=[vs[0] for vs in dset_values])
    jtags += make_histo(overhead_times, num_bins, 'Overhead time (m)', 'Jobs', 'overhead_time', top_dir, label=[vs[0] for vs in dset_values])
    jtags += make_histo(stageout_times, num_bins, 'Stage-out time (m)', 'Jobs', 'stageout_time', top_dir, label=[vs[0] for vs in dset_values])

    # hosts = vals['host']
    # host_clusters = np.char.rstrip(np.char.replace(vals['host'], '.crc.nd.edu', ''), '0123456789-')

    # web.update_indexes(args.outdir)
    # raise

    print "Reading WQ log"
    with open(os.path.join(args.directory, 'work_queue.log')) as f:
        headers = dict(map(lambda (a, b): (b, a), enumerate(f.readline()[1:].split())))
    wq_stats_raw = np.loadtxt(os.path.join(args.directory, 'work_queue.log'))
    orig_times = wq_stats_raw[:,0].copy()
    # subtract start time, convert to minutes
    start_time = wq_stats_raw[0,0]
    end_time = wq_stats_raw[-1,0]
    wq_stats_raw[:,0] = (wq_stats_raw[:,0] - start_time) / 60e6
    runtimes = wq_stats_raw[:,0]
    print "First iteration..."
    print "start_time = ",int(start_time)

    if args.xmax is not None:
        bins = xrange(args.xmin, int(args.xmax) + 5, 5)
    else:
        bins = xrange(args.xmin, int(runtimes[-1]) + 5, 5)

    wtags += make_histo([runtimes], bins, 'Time (m)', 'Activity', 'activity', top_dir, log=True)

    transferred = (wq_stats_raw[:,headers['total_bytes_received']] - np.roll(wq_stats_raw[:,headers['total_bytes_received']], 1, 0)) / 1024**3
    transferred[0] = 0
    if args.xmax is not None:
        bins = xrange(args.xmin, int(args.xmax) + 60, 60)
    else:
        bins = xrange(args.xmin, int(runtimes[-1]) + 60, 60)
    wtags += make_histo([runtimes], bins, 'Time (m)', 'Output (GB/h)', 'rate', top_dir, weights=[transferred])

    # gap_indices = np.logical_or((np.roll(runtimes, -1) - runtimes) > 5, (runtimes - np.roll(runtimes, 1)) > 5)
    # gap_indices = np.logical_or(gap_indices, np.logical_or(np.roll(gap_indices, -1), np.roll(gap_indices, 1)))
    # print len(wq_stats_raw[gap_indices])
    # np.savetxt('gaps.txt', orig_times[gap_indices], '%30.1f')
    # sys.exit()

    print "Reducing WQ log"
    wq_stats = reduce(wq_stats_raw, 0, 5.)
    runtimes = wq_stats[:,0]

    wtags += make_plot(([(runtimes, wq_stats[:,headers['workers_busy']], 'busy'),
               (runtimes, wq_stats[:,headers['workers_idle']], 'idle'),
               (runtimes, wq_stats[:,headers['total_workers_connected']], 'connected')],
               [(runtimes, wq_stats[:,headers['tasks_running']], 'running')]),
               'Time (m)', 'Workers' , 'workers_active', top_dir, y_label2='Tasks', xmin=args.xmin, xmax=args.xmax)


    if args.xmax is not None:
        bins = xrange(args.xmin, int(args.xmax) + 5, 5)
    else:
        bins = xrange(args.xmin, int(runtimes[-1]) + 5, 5)
    success_times = (success_jobs['t_retrieved'] - start_time / 1e6) / 60
    failed_times = (failed_jobs['t_retrieved'] - start_time / 1e6) / 60
    #print failed_times
    wtags += make_histo([success_times, failed_times], bins, 'Time (m)', 'Jobs', 'jobs', top_dir, label=['succesful', 'failed'], color=['green', 'red'])

    with open(os.path.join(top_dir, 'index.html'), 'w') as f:
        body = html_tag("div",
                *([html_tag("h2", "Job Statistics")] +
                    map(lambda t: html_tag("div", t, style="clear: both;"), jtags) +
                    [html_tag("h2", "Lobster Statistics")] +
                    map(lambda t: html_tag("div", t, style="clear: both;"), wtags)),
                style="margin: 1em auto; display: block; width: auto; text-align: center;")
        f.write(body)
