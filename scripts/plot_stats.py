#!/usr/bin/env python
# vim: fileencoding=utf-8

from argparse import ArgumentParser
from os.path import expanduser
from collections import defaultdict
from datetime import datetime
import glob
import math
import os
import pytz
import sqlite3
import gzip

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import numpy as np

matplotlib.rc('axes', labelsize='large')
matplotlib.rc('figure', figsize=(8, 1.5))
matplotlib.rc('figure.subplot', left=0.09, right=0.92, bottom=0.275)
matplotlib.rc('font', size=7)
matplotlib.rc('font', **{'sans-serif' : 'DejaVu LGC Sans', 'family' : 'sans-serif'})

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

def html_table(headers, rows, **kwargs):
    import itertools
    row_classes = itertools.cycle(["tr class=alt", "tr"])

    top = [html_tag('tr', *(html_tag('th', h) for h in headers))]
    body = [html_tag(rc, *(html_tag('td', x) for x in row)) for rc, row in itertools.izip(row_classes, rows)]

    return html_tag('table', *(top+body), **kwargs)

def unix2matplotlib(time):
    return dates.date2num(datetime.fromtimestamp(time, pytz.utc))

def make_histo(a, num_bins, xlabel, ylabel, filename, dir, vs_time=False, **kwargs):
    # fig, (ax0, ax1) = plt.subplots(nrows=2, sharex=True)

    fig, ax = plt.subplots()

    if vs_time:
        f = np.vectorize(unix2matplotlib)
        a = map(lambda xs: f(xs), a)
        interval = 2**math.floor(math.log((num_bins[-1] - num_bins[0]) / 9000.0) / math.log(2))
        num_bins = map(unix2matplotlib, num_bins)
        ax.xaxis.set_major_locator(dates.MinuteLocator(byminute=range(0, 60, 15), interval=interval))
        ax.xaxis.set_major_formatter(dates.DateFormatter("%H:%M"))

    if 'log' in kwargs:
        if kwargs['log'] == True or kwargs['log'] == 'y':
            ax.set_yscale('log')
        elif kwargs['log'] == 'x':
            ax.set_xscale('log')
        del kwargs['log']

    if 'stats' in kwargs:
        stats = kwargs['stats']
        del kwargs['stats']
    else:
        stats = False

    if 'histtype' in kwargs:
        ax.hist(a, bins=num_bins, **kwargs)
    else:
        ax.hist(a, bins=num_bins, histtype='barstacked', **kwargs)

    ax.grid(True)

    if stats:
        all = np.concatenate(a)
        avg = np.average(all)
        var = np.std(all)
        med = np.median(all)
        ax.text(0.75, 0.7,
                u"μ = {0:.3g}, σ = {1:.3g}\nmedian = {2:.3g}".format(avg, var, med),
                ha="center", transform=ax.transAxes, backgroundcolor='white')

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)

    try:
        ax.axis(xmin=num_bins[0], xmax=num_bins[-1])
    except:
        pass

    if 'label' in kwargs:
        ax.legend(bbox_to_anchor=(0.5, 0.9), loc='lower center', ncol=len(kwargs['label']), prop={'size': 7})

    return save_and_close(dir, filename)

def make_frequency_pie(a, name, dir, threshold=0.05):
    vals = np.unique(a)
    counts = [len(a[a == v]) for v in vals]
    total = sum(counts)

    counts, vals = zip(*filter(lambda (c, l): c / float(total) >= threshold, zip(counts, vals)))
    rest = total - sum(counts)

    plt.pie(counts + (rest, ), labels=vals + ('Other', ))
    fig = plt.gcf()
    fig.set_size_inches(3, 3)
    fig.subplots_adjust(left=0.05, bottom=0.05, right=0.95, top=0.95)

    return save_and_close(dir, name)

def make_plot(tuples, x_label, y_label, name, dir, fun=matplotlib.axes.Axes.plot, y_label2=None, log=False, vs_time=False, **kwargs):
    fig, ax1 = plt.subplots()

    if log == True or log == 'y':
        ax1.set_yscale('log')
    elif log == 'x':
        ax1.set_xscale('log')

    plots1 = tuples[0] if y_label2 else tuples

    if vs_time:
        f = np.vectorize(unix2matplotlib)
        interval = 2**math.floor(math.log((plots1[0][0][-1] - plots1[0][0][0]) / 9000.0) / math.log(2))
        ax1.xaxis.set_major_locator(dates.MinuteLocator(byminute=range(0, 60, 15), interval=interval))
        ax1.xaxis.set_major_formatter(dates.DateFormatter("%H:%M"))

    for x, y, l in plots1:
        if vs_time:
            x = f(x)
        fun(ax1, x, y, label=l)

    ax1.set_xlabel(x_label)
    ax1.set_ylabel(y_label)
    ax1.grid(True)

    if y_label2:
        ax2 = ax1.twinx()
        if vs_time:
            ax2.xaxis.set_major_locator(dates.MinuteLocator(byminute=range(0, 60, 15), interval=interval))
            ax2.xaxis.set_major_formatter(dates.DateFormatter("%H:%M"))

        for x, y, l in tuples[1]:
            if vs_time:
                x = f(x)
            fun(ax2, x, y, ':', label=l)
        ax2.set_ylabel(y_label2)
        ax2.legend(bbox_to_anchor=(0.975, 0.9),
                loc='lower right',
                ncol=len(tuples[1]),
                prop={'size': 7})

    ax1.legend(bbox_to_anchor=(0.025 if y_label2 else 0.5, 0.9),
            loc='lower left' if y_label2 else 'lower center',
            ncol=len(plots1),
            prop={'size': 7})

    return save_and_close(dir, name)

def make_profile(x, y, bins, xlabel, ylabel, name, dir, vs_time=False):
    fig, ax = plt.subplots()

    if vs_time:
        f = np.vectorize(unix2matplotlib)
        x = map(lambda xs: f(xs), x)
        interval = 2**math.floor(math.log((bins[-1] - bins[0]) / 9000.0) / math.log(2))
        bins = map(unix2matplotlib, bins)
        ax.xaxis.set_major_locator(dates.MinuteLocator(byminute=range(0, 60, 15), interval=interval))
        ax.xaxis.set_major_formatter(dates.DateFormatter("%H:%M"))

    sums, edges = np.histogram(x, bins=bins, weights=y)
    squares, edges = np.histogram(x, bins=bins, weights=np.multiply(y, y))
    counts, edges = np.histogram(x, bins=bins)
    avg = np.divide(sums, counts)
    avg_sq = np.divide(squares, counts)
    err = np.sqrt(np.subtract(avg_sq, np.multiply(avg, avg)))

    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    centers = [(x + y) / 2.0 for x, y in zip(edges[:-1], edges[1:])]
    ax.errorbar(centers, avg, yerr=err, fmt='o', ms=3, capsize=0)
    ax.axis(xmin=bins[0], xmax=bins[-1], ymin=0)
    ax.grid(True)

    return save_and_close(dir, name)

def make_scatter(x, y, bins, xlabel, ylabel, name, dir, yrange=None):
    plt.hexbin(x, y, cmap=plt.cm.Purples, gridsize=(len(bins) - 1, 10))
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    if yrange is not None:
        plt.ylim(yrange)
    plt.axis(xmax=bins[-1])

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

def split_by_column(a, col, key=lambda x: x, threshold=None):
    """Split an array into multiple ones, based on unique values in the named
    column `col`.
    """
    keys = np.unique(a[col])
    vals = [a[a[col] == v] for v in keys]
    keys = map(key, keys)

    if threshold:
        total = float(len(a))
        others = filter(lambda v: len(v) / total < threshold, vals)
        keys, vals = zip(*filter(lambda (k, v): len(v) / total >= threshold, zip(keys, vals)))
        if len(others) > 0:
            keys += ("Other", )
            vals += (np.concatenate(others), )

    return keys, vals

def save_and_close(dir, name):
    if not os.path.exists(dir):
        os.makedirs(dir)
    print "Saving", name
    # plt.gcf().set_size_inches(6, 1.5)

    plt.savefig(os.path.join(dir, '%s.png' % name))
    # plt.savefig(os.path.join(dir, '%s.pdf' % name))

    plt.close()

    return html_tag("img", src='{0}.png'.format(name))

if __name__ == '__main__':
    parser = ArgumentParser(description='make histos')
    parser.add_argument('directory', help="Specify input directory")
    parser.add_argument('outdir', nargs='?', help="Specify output directory")
    parser.add_argument("--xmin", type=int, help="Specify custom x-axis minimum", default=0, metavar="MIN")
    parser.add_argument("--xmax", type=int, help="Specify custom x-axis maximum", default=None, metavar="MAX")
    parser.add_argument('--samplelogs', action='store_true', help='Make a table with links to sample error logs', default=False)
    args = parser.parse_args()

    if args.outdir:
        top_dir = args.outdir
    else:
        top_dir = os.path.join(os.environ['HOME'], 'www',  os.path.basename(os.path.normpath(args.directory)))

    print 'Saving plots to: ' + top_dir
    if not os.path.isdir(top_dir):
        os.makedirs(top_dir)

    jtags = SmartList()
    dtags = SmartList()
    wtags = SmartList()

    print "Reading WQ log"
    with open(os.path.join(args.directory, 'work_queue.log')) as f:
        headers = dict(map(lambda (a, b): (b, a), enumerate(f.readline()[1:].split())))
    wq_stats_raw_all = np.loadtxt(os.path.join(args.directory, 'work_queue.log'))
    start_time = wq_stats_raw_all[0,0] / 1e6
    end_time = wq_stats_raw_all[-1,0]

    if not args.xmax:
        xmax = end_time
    else:
        xmax = args.xmax * 60 + start_time

    xmin = args.xmin * 60 + start_time

    wq_stats_raw = wq_stats_raw_all[np.logical_and(wq_stats_raw_all[:,0] >= xmin, wq_stats_raw_all[:,0] <= xmax),:]

    orig_times = wq_stats_raw[:,0].copy()
    # Convert to seconds since UNIX epoch
    runtimes = wq_stats_raw[:,0] / 1e6
    print "First iteration..."

    # Five minute bins (or more, if # of bins exceeds 100)
    bins = range(int(xmin), int(runtimes[-1]) + 300, 300)
    scale = max(len(bins) / 100.0, 1.0)
    bins = range(int(xmin), int(runtimes[-1]) + scale * 300, scale * 300)
    wtags += make_histo([runtimes], bins, 'Time (m)', 'Activity', 'activity', top_dir, log=True, vs_time=True)

    transferred = (wq_stats_raw[:,headers['total_bytes_received']] - np.roll(wq_stats_raw[:,headers['total_bytes_received']], 1, 0)) / 1024**3
    transferred[transferred < 0] = 0

    # One hour bins
    bins = range(int(xmin), int(runtimes[-1]) + 3600, 3600)
    wtags += make_histo([runtimes], bins, 'Time (m)', 'Output (GB/h)', 'rate', top_dir, weights=[transferred], vs_time=True)

    print "Reducing WQ log"
    wq_stats = reduce(wq_stats_raw, 0, 300.)
    runtimes = wq_stats[:,0] / 1e6

    wtags += make_plot(([(runtimes, wq_stats[:,headers['workers_busy']], 'busy'),
               (runtimes, wq_stats[:,headers['workers_idle']], 'idle'),
               (runtimes, wq_stats[:,headers['total_workers_connected']], 'connected')],
               [(runtimes, wq_stats[:,headers['tasks_running']], 'running')]),
               'Time (m)', 'Workers' , 'workers_active', top_dir, y_label2='Tasks', vs_time=True)

    db = sqlite3.connect(os.path.join(args.directory, 'lobster.db'))
    stats = {}

    failed_jobs = np.array(db.execute("""select
        id,
        host,
        dataset,
        exit_code,
        time_submit,
        time_retrieved,
        time_total_on_worker
        from jobs
        where status=3 and time_retrieved>=? and time_retrieved<=?""",
        (xmin / 1e6, xmax / 1e6)).fetchall(),
            dtype=[
                ('id', 'i4'),
                ('host', 'a50'),
                ('dataset', 'i4'),
                ('exit_code', 'i4'),
                ('t_submit', 'i4'),
                ('t_retrieved', 'i4'),
                ('t_allput', 'i8')
                ])

    success_jobs = np.array(db.execute("""select * from jobs
        where (status=2 or status=5 or status=6) and time_retrieved>=? and time_retrieved<=?""",
        (xmin / 1e6, xmax / 1e6)).fetchall(),
            dtype=[
                ('id', 'i4'),
                ('host', 'a50'),
                ('dataset', 'i4'),
                ('file_block', 'a100'),
                ('status', 'i4'),
                ('exit_code', 'i4'),
                ('retries', 'i4'),
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
                ('t_goodput', 'i8'),
                ('t_allput', 'i8'),
                ('b_recv', 'i4'),
                ('b_sent', 'i4')
                ])

    total_time_failed = np.sum(failed_jobs['t_allput'])
    total_time_success = np.sum(success_jobs['t_allput'])
    total_time_good = np.sum(success_jobs['t_goodput'])
    total_time_pure = np.sum(success_jobs['t_wrapper_end'] - success_jobs['t_first_ev']) * 1e6

    # Five minute bins, or larger, to keep the number of bins around 100
    # max.
    bins = xrange(int(xmin), int(runtimes[-1]) + 300, 300)
    scale = max(len(bins) / 100.0, 1.0)
    bins = xrange(int(xmin), int(runtimes[-1]) + scale * 300, scale * 300)
    success_times = (success_jobs['t_retrieved'] - start_time / 1e6)
    failed_times = (failed_jobs['t_retrieved'] - start_time / 1e6)

    wtags += make_histo([success_times, failed_times], bins, 'Time (m)', 'Jobs', 'jobs', top_dir, label=['succesful', 'failed'], color=['green', 'red'], vs_time=True)
    wtags += make_profile(
            success_jobs['t_wrapper_start'],
            (success_jobs['t_first_ev'] - success_jobs['t_wrapper_start']) / 60.,
            bins, 'Wrapper start time (m)', 'Overhead (m)', 'overhead_vs_time', top_dir, vs_time=True)

    fail_labels, fail_values = split_by_column(failed_jobs, 'exit_code', threshold=0.025)
    fail_times = [vs['t_retrieved'] / 1e6 for vs in fail_values]
    wtags += make_histo(fail_times, bins, 'Time (m)', 'Jobs',
            'fail_times', top_dir, label=map(str, fail_labels), vs_time=True)

    # for cases where jobits per job changes during run, get per-jobit info
    total_jobits = db.execute('select sum(jobits) from datasets').fetchone()[0]
    success_jobits = []
    for (label,) in db.execute("select label from datasets"):
        success_jobits.append(np.array(db.execute("""
            select jobits_{0}.id, jobs.time_retrieved
            from jobits_{0}, jobs
            where jobits_{0}.job == jobs.id
                and (jobits_{0}.status=2 or jobits_{0}.status=5 or jobits_{0}.status=6)
                and jobs.time_retrieved>=? and jobs.time_retrieved<=?""".format(label),
            (xmin, xmax)).fetchall(),
                dtype=[('id', 'i4'), ('t_retrieved', 'i4')]))

    success_jobits = np.concatenate(success_jobits)
    finished_jobit_times = success_jobits['t_retrieved']
    finished_jobit_hist, jobit_bins = np.histogram(finished_jobit_times, bins)
    bin_centers = [(x+y)/2 for x, y in zip(jobit_bins[:-1], jobit_bins[1:])]
    finished_jobit_cum = np.cumsum(finished_jobit_hist)

    if any([x>0 for x in finished_jobit_cum]):
        wtags += make_plot([(bin_centers, finished_jobit_cum, 'total finished'),
                            (bin_centers, finished_jobit_cum * (-1) + total_jobits, 'total unfinished')],
                           'Time (m)', 'Jobits' , 'finished_jobits', top_dir, log=True, vs_time=True)

    label2id = {}
    id2label = {}
    total_events = {}
    processed_events = {}

    for dset_label, dset_id, t_events, p_events in db.execute("""select
            label,
            id,
            total_events,
            processed_events
            from datasets"""):
        label2id[dset_label] = dset_id
        id2label[dset_id] = dset_label
        total_events[dset_label] = t_events
        processed_events[dset_label] = p_events

    dset_labels, dset_values = split_by_column(success_jobs, 'dataset', key=lambda x: id2label[x])

    num_bins = 30
    total_times = [(vs['t_wrapper_end'] - vs['t_wrapper_start']) / 60. for vs in dset_values]
    processing_times = [(vs['t_wrapper_end'] - vs['t_first_ev']) / 60. for vs in dset_values]
    overhead_times = [(vs['t_first_ev'] - vs['t_wrapper_start']) / 60. for vs in dset_values]
    idle_times = [(vs['t_wrapper_start'] - vs['t_send_end']) / 60. for vs in dset_values]
    init_times = [(vs['t_wrapper_ready'] - vs['t_wrapper_start']) / 60. for vs in dset_values]
    cmsrun_times = [(vs['t_first_ev'] - vs['t_wrapper_ready']) / 60. for vs in dset_values]

    stageout_times = [(vs['t_retrieved'] - vs['t_wrapper_end']) / 60. for vs in dset_values]
    wait_times = [(vs['t_recv_start'] - vs['t_wrapper_end']) / 60. for vs in dset_values]
    transfer_times = [(vs['t_recv_end'] - vs['t_recv_start']) / 60. for vs in dset_values]
    transfer_bytes = [vs['b_recv'] / 1024.0**2 for vs in dset_values]
    transfer_rates = []
    for (bytes, times) in zip(transfer_bytes, transfer_times):
        transfer_rates.append(np.divide(bytes[times != 0], times[times != 0] * 60.))

    send_times = [(vs['t_send_end'] - vs['t_send_start']) / 60. for vs in dset_values]
    send_bytes = [vs['b_sent'] / 1024.0**2 for vs in dset_values]
    send_rates = []
    for (bytes, times) in zip(send_bytes, send_times):
        send_rates.append(np.divide(bytes[times != 0], times[times != 0] * 60.))
    put_ratio = [np.divide(vs['t_goodput'] * 1.0, vs['t_allput']) for vs in dset_values]
    pureput_ratio = [np.divide((vs['t_wrapper_end'] -  vs['t_first_ev']) * 1e6, vs['t_allput']) for vs in dset_values]


    jtags += make_histo(total_times, num_bins, 'Runtime (m)', 'Jobs', 'run_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(processing_times, num_bins, 'Pure processing time (m)', 'Jobs', 'processing_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(overhead_times, num_bins, 'Overhead time (m)', 'Jobs', 'overhead_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(idle_times, num_bins, 'Idle time (m) - End receive job data to wrapper start', 'Jobs', 'idle_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(init_times, num_bins, 'Wrapper initialization time (m)', 'Jobs', 'wrapper_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(cmsrun_times, num_bins, 'cmsRun startup time (m)', 'Jobs', 'cmsrun_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(stageout_times, num_bins, 'Stage-out time (m)', 'Jobs', 'stageout_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(wait_times, num_bins, 'Wait time (m)', 'Jobs', 'wait_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(transfer_times, num_bins, 'Transfer time (m)', 'Jobs', 'transfer_time', top_dir, label=dset_labels, stats=True)
    jtags += make_histo(transfer_bytes,
            num_bins, 'Data received (MiB)', 'Jobs', 'recv_data', top_dir,
            label=dset_labels, stats=True)
    jtags += make_histo(transfer_rates,
            num_bins, 'Data received rate (MiB/s)', 'Jobs', 'recv_rate', top_dir,
            label=dset_labels, stats=True)

    if args.samplelogs:
        jtags += html_tag('a', make_frequency_pie(failed_jobs['exit_code'], 'exit_codes', top_dir), href='errors.html')
    else:
        jtags += make_frequency_pie(failed_jobs['exit_code'], 'exit_codes', top_dir)

    dtags += make_histo(send_times, num_bins, 'Send time (m)', 'Jobs', 'send_time', top_dir, label=dset_labels, stats=True)
    dtags += make_histo(send_bytes,
            num_bins, 'Data sent (MiB)', 'Jobs', 'send_data', top_dir,
            label=dset_labels, stats=True)
    dtags += make_histo(send_rates,
            num_bins, 'Data sent rate (MiB/s)', 'Jobs', 'send_rate', top_dir,
            label=dset_labels, stats=True)
    # dtags += make_histo(put_ratio, num_bins, 'Goodput / (Goodput + Badput)', 'Jobs', 'put_ratio', top_dir, label=[vs[0] for vs in dset_values], stats=True)
    dtags += make_histo(put_ratio, [0.05 * i for i in range(21)], 'Goodput / (Goodput + Badput)', 'Jobs', 'put_ratio', top_dir, label=dset_labels, stats=True)
    dtags += make_histo(pureput_ratio, [0.05 * i for i in range(21)], 'Pureput / (Goodput + Badput)', 'Jobs', 'pureput_ratio', top_dir, label=dset_labels, stats=True)

    events_remaining = dict((dl, total_events[dl]-processed_events[dl]) for dl in dset_labels)
    event_stats = [[dl, total_events[dl], events_remaining[dl], processed_events[dl], '%.0f%%' % (processed_events[dl] / float(total_events[dl]) * 100)] for dl in dset_labels]

    # hosts = vals['host']
    # host_clusters = np.char.rstrip(np.char.replace(vals['host'], '.crc.nd.edu', ''), '0123456789-')

    header = """
             <style>
             body {font-family:"Trebuchet MS";}

             table
             {width:auto;
             margin-left:auto;
             margin-right:auto;
             margin-bottom:50px;
             border-collapse:collapse;}

             td, th
             {border:1px solid #333437;
             padding:3px 7px 2px 7px;}

             th
             {text-align:left;
             padding-top:5px;
             padding-bottom:4px;
             background-color:#454648;
             color:#ffffff;}

             tr.alt td
             {color:#000000;
             background-color:#B4B8C1;}
             </style>"""

    with open(os.path.join(top_dir, 'index.html'), 'w') as f:
        body = html_tag("div",
                *([html_tag("h2", "Job Statistics")] +
                  [html_table(['dataset', 'total events', 'remaining events', 'processed events', 'percent processed'], [[str(x) for x in ds] for ds in event_stats])] +
                    map(lambda t: html_tag("div", t, style="clear: both;"), jtags) +
                    [html_tag("h2", "Debug Job Statistics")] +
                    map(lambda t: html_tag("div", t, style="clear: both;"), dtags) +
                    [
                        html_tag("h2", "Lobster Statistics"),
                        html_tag("p", "Successful jobs: Goodput / Allput = {0:.3f}".format(total_time_good / float(total_time_success))),
                        html_tag("p", "Successful jobs: Pureput / Allput = {0:.3f}".format(total_time_pure / float(total_time_success))),
                        html_tag("p", "All jobs: Goodput / Allput = {0:.3f}".format(total_time_good / float(total_time_success + total_time_failed))),
                        html_tag("p", "All jobs: Pureput / Allput = {0:.3f}".format(total_time_pure / float(total_time_success + total_time_failed)))
                    ] +
                    map(lambda t: html_tag("div", t, style="clear: both;"), wtags)),
                style="margin: 1em auto; display: block; width: auto; text-align: center;")
        f.write(header+body)

    if args.samplelogs:
        with open(os.path.join(top_dir, 'errors.html'), 'w') as f:
            f.write(header)

            if not os.path.exists(os.path.join(top_dir, 'errors')):
                os.makedirs(os.path.join(top_dir, 'errors'))

            import shutil
            headers = []
            rows = [[], [], [], [], []]
            num_samples = 5
            for exit_code, jobs in zip(split_by_column(failed_jobs[['id', 'dataset', 'exit_code']], 'exit_code')):
                headers.append('Exit %i <br>(%i failed jobs)' % (exit_code, len(jobs)))
                print 'Copying sample logs for exit code ', exit_code
                for row, j in enumerate(list(jobs[:num_samples])+[()]*(num_samples-len(jobs))):
                    if len(j) == 0:
                        rows[row].append('')
                    else:
                        id, ds, e = j
                        from_path = os.path.join(args.directory, id2label[ds], 'failed', str(id))
                        to_path = os.path.join(os.path.join(top_dir, 'errors'), str(id))
                        if os.path.exists(to_path):
                            shutil.rmtree(to_path)
                        os.makedirs(to_path)
                        cell = []
                        for l in ['cmssw.log.gz', 'job.log.gz']:
                            if os.path.exists(os.path.join(from_path, l)):
                                shutil.copy(os.path.join(from_path, l), os.path.join(to_path, l))
                                os.popen('gunzip %s' % os.path.join(to_path, l)) #I don't know how to make our server serve these correctly
                                cell.append(html_tag('a', l.replace('.gz', ''), href=os.path.join('errors', str(id), l.replace('.gz', ''))))
                        rows[row].append(', '.join(cell))

            f.write(html_table(headers, rows))

