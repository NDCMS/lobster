# vim: fileencoding=utf-8

from collections import defaultdict, Counter
from cycler import cycler
from datetime import datetime
import glob
import gzip
import itertools
import jinja2
import logging
import multiprocessing
import os
import pickle
import shutil
import sqlite3
import signal
import time
import re
import string
import json

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import matplotlib.ticker as ticker
import numpy as np

from scipy.interpolate import UnivariateSpline

from lobster import util
from lobster.core import unit
from lobster.core.command import Command

from WMCore.DataStructs.LumiList import LumiList

matplotlib.rc('axes', labelsize='large')
matplotlib.rc('figure', figsize=(8, 1.5))
matplotlib.rc('figure.subplot', left=0.09, right=0.96, bottom=0.275)
matplotlib.rc('hatch', linewidth=.3)
matplotlib.rc('font', size=7)
matplotlib.rc('font', **{'sans-serif': 'Liberation Sans', 'family': 'sans-serif'})

logger = logging.getLogger('lobster.plotting')


def reset_signals():
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def reduce(a, idx, interval):
    quant = a[:, idx]
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
        keys, vals = zip(*filter(lambda (k, v): len(v) /
                                 total >= threshold, zip(keys, vals)))
        if len(others) > 0:
            keys += ("Other", )
            vals += (np.concatenate(others), )

    return keys, vals


def unix2matplotlib(time):
    return dates.date2num(datetime.fromtimestamp(time))


def unpack(arg):
    source, target = arg
    try:
        if os.path.isfile(target):
            logger.info("skipping {0}".format(source))
            return
        logger.info("unpacking {0}".format(source))
        with open(target, 'w') as output:
            input = gzip.open(source, 'rb')
            output.writelines(input)
            input.close()
    except IOError:
        logger.error("cannot unpack {0}".format(source))


def mp_call(arg):
    fct, args, kwargs = arg
    try:
        fct(*args, **kwargs)
    except Exception as e:
        logger.error('method {0} failed with "{1}", using args {2}, {3}'.format(fct, e, args, kwargs))
        logger.exception(e)


def mp_pickle(plotdir, name, data):
    logger.debug("Saving data for {0}".format(name))
    with open(os.path.join(plotdir, name + '.pkl'), 'wb') as f:
        pickle.dump(data, f)


def mp_pie(vals, labels, name, plotdir=None, **kwargs):
    vals = [max(0, val) for val in vals]

    fig, ax = plt.subplots()
    fig.set_size_inches(4, 3)
    ratio = 0.75
    ax.set_position([0.3, 0.3, ratio * 0.7, 0.7])

    paper = kwargs.pop('paper', False)
    if paper:
        plt.rcParams['patch.force_edgecolor'] = True
        if 'colors' in kwargs:
            del kwargs['colors']
        hatching = [p * 5 for p in ['/', '\\', '|', '-', 'x', '+', '*']]
        monochrome = \
            cycler('hatch', hatching) * \
            cycler('edgecolor', ['k']) * \
            cycler('color', ['w']) * \
            cycler('linewidth', [.5])
        ax.set_prop_cycle(monochrome)

    newlabels = []
    total = sum(vals)
    for label, val in zip(labels, vals):
        if float(val) / total < .025:
            newlabels.append('')
        else:
            newlabels.append(label)

    with open(os.path.join(plotdir, name + '.dat'), 'w') as f:
        for l, v in zip(labels, vals):
            f.write('{0}\t{1}\n'.format(l, v))

    patches, texts = ax.pie([max(0, val) for val in vals], labels=newlabels, **kwargs)

    if paper:
        for p, h in zip(patches, itertools.cycle(hatching)):
            p.set_hatch(h)
            p.set_linewidth(.5)

    boxes = []
    newlabels = []
    for patch, text, label in zip(patches, texts, labels):
        if isinstance(label, basestring) and len(text.get_text()) == 0 and len(label) > 0:
            boxes.append(patch)
            newlabels.append(label)

    if len(boxes) > 0:
        ax.legend(boxes, newlabels, ncol=2, mode='expand',
                  bbox_to_anchor=(0, 0, 1, .3),
                  bbox_transform=plt.gcf().transFigure,
                  title='Small Slices',
                  prop={'size': 6})

    return mp_saveimg(plotdir, name)


def smooth_data(a):
    b = []
    for (x, y) in a:
        if isinstance(x, list):
            x = np.array(x)
        if isinstance(y, list):
            y = np.array(y)
        if len(x) == 0:
            b.append(([], []))
            continue
        s = np.linspace(x.min(), x.max(), 400)
        t = UnivariateSpline(x, y, s=.1)
        b.append((s, t(s)))
        del t
    return b


class BetterFormatter(ticker.ScalarFormatter):

    def __init__(self):
        super(BetterFormatter, self).__init__(useMathText=True)
        self.set_powerlimits((-3, 7))

    def pprint_val(self, value):
        if round(value, 0) == value:
            value = int(value)
        if self.orderOfMagnitude != 0:
            value /= 10.0 ** self.orderOfMagnitude
        return "{:,}".format(value)


def mp_plot(a, xlabel, stub=None, ylabel='tasks', bins=50, modes=None, ymax=None, xmin=None, xmax=None, plotdir=None, **kwargs):
    if not modes:
        modes = [Plotter.PROF | Plotter.TIME, Plotter.HIST]

    paper = kwargs.pop('paper', False)
    oldxlabel = xlabel
    oldylabel = ylabel

    for mode in modes:
        filename = stub
        fig, ax = plt.subplots()
        hatching = []

        xlabel = oldxlabel
        ylabel = oldylabel

        ax.yaxis.set_major_formatter(BetterFormatter())

        # to pickle plot contents
        data = {'data': a, 'bins': bins, 'labels': kwargs.get('label')}

        if paper:
            hatching = [p * 8 for p in ['/', '\\', '|', '-', 'x', '+', '*']]
            if 'color' in kwargs:
                del kwargs['color']
            if mode & Plotter.HIST:
                monochrome = \
                    cycler('hatch', hatching) * \
                    cycler('edgecolor', ['k']) * \
                    cycler('color', ['w']) * \
                    cycler('linewidth', [.5])
                ax.set_prop_cycle(monochrome)
            elif mode & Plotter.STACK:
                a = smooth_data(a)
                monochrome = \
                    cycler('hatch', hatching) * \
                    cycler('edgecolor', ['k']) * \
                    cycler('facecolor', ['w']) * \
                    cycler('linewidth', [.5])
                ax.set_prop_cycle(monochrome)
            elif mode & Plotter.PROF:
                kwargs['linewidth'] = .8
                monochrome = \
                    cycler('color', ['k', 'gray']) * \
                    cycler('marker', ['o', 'v', '^', 's', 'P', 'X', 'D'])
                ax.set_prop_cycle(monochrome)
            else:
                a = smooth_data(a)
                monochrome = \
                    cycler('color', ['k', 'gray']) * \
                    cycler('linewidth', [1.]) * \
                    cycler('linestyle', ['-', '--', ':', '-.']) * \
                    cycler('marker', [''])
                ax.set_prop_cycle(monochrome)

        if mode & Plotter.TIME:
            f = np.vectorize(unix2matplotlib)
            a = [(f(x), y) for (x, y) in a if len(x) > 0]

            data['data'] = a

            # interval = 2**math.floor(math.log((bins[-1] - bins[0]) / 9000.0) / math.log(2))
            # num_bins = map(unix2matplotlib, bins)
            # ax.xaxis.set_major_locator(dates.MinuteLocator(byminute=range(0, 60, 15), interval=24*60))
            ax.xaxis.set_major_formatter(dates.DateFormatter("%m-%d\n%H:%M"))
            ylabel = xlabel
        else:
            ax.set_xlabel(xlabel)
        ax.set_ylabel(ylabel)

        if mode & Plotter.HIST:
            filename += '-hist'

            if mode & Plotter.TIME:
                borders = (unix2matplotlib(xmin), unix2matplotlib(xmax))
                count, bins, patches = ax.hist([x for (x, y) in a], weights=[y for (x, y) in a],
                                               bins=bins, histtype='stepfilled', stacked=True,
                                               range=borders, **kwargs)
                if '/' not in ylabel:
                    ax.set_ylabel('{} / {:.0f} min'.format(ylabel,
                                                           (bins[1] - bins[0]) * 24 * 60.))
            else:
                ax.hist([y for (x, y) in a], bins=bins,
                        histtype='stepfilled', stacked=True, **kwargs)
        elif mode & Plotter.PROF:
            filename += '-prof'
            data['data'] = []

            markers = itertools.cycle(['o', 'v', '^', 's', 'P', 'X', 'D'])

            for i, (x, y) in enumerate(a):
                borders = (unix2matplotlib(xmin), unix2matplotlib(xmax))
                sums, edges = np.histogram(
                    x, bins=bins, range=borders, weights=y)
                squares, edges = np.histogram(
                    x, bins=bins, range=borders, weights=np.multiply(y, y))
                counts, edges = np.histogram(x, bins=bins, range=borders)
                avg = np.divide(sums, counts)
                avg_sq = np.divide(squares, counts)
                err = np.sqrt(np.subtract(avg_sq, np.multiply(avg, avg)))
                sel = counts > 0

                newargs = dict(kwargs)
                if 'color' in newargs:
                    newargs['color'] = newargs['color'][i]
                if 'label' in newargs:
                    newargs['label'] = newargs['label'][i]

                centers = np.array([.5 * (x + y)
                                    for x, y in zip(edges[:-1], edges[1:])])
                ax.errorbar(centers[sel], avg[sel], yerr=err[sel],
                            fmt='o', marker=next(markers), ms=3, capsize=0, **newargs)

                data['data'].append((centers, avg, err))

        elif mode & Plotter.PLOT:
            filename += '-plot'

            if 'label' in kwargs:
                for (l, (x, y)) in zip(kwargs['label'], a):
                    ax.plot(x, y, label=l)
            else:
                for (x, y) in a:
                    ax.plot(x, y)
        elif mode & Plotter.STACK:
            filename += '-stack'
            t = []
            ys = []
            for x, y in a:
                ys.append(y)
                t = x
            if not paper:
                kwargs['edgecolor'] = 'none'
            ps = ax.stackplot(t, *ys, **kwargs)
            if paper:
                for p, h in zip(ps, itertools.cycle(hatching)):
                    p.set_edgecolor('k')
                    p.set_facecolor('w')
                    p.set_hatch(h)
                    p.set_linewidth(.5)

        ax.grid(True)

        if mode & Plotter.TIME:
            ax.axis(xmin=unix2matplotlib(xmin),
                    xmax=unix2matplotlib(xmax), ymin=0)
        else:
            ax.axis(ymin=0)

        if ymax:
            ax.axis(ymax=ymax)

        if not mode & Plotter.TIME and mode & Plotter.HIST and not paper:
            labels = kwargs.get('label', [''] * len(a))
            stats = {}
            for label, (x, y) in zip(labels, a):
                avg = np.average(y)
                var = np.std(y)
                med = np.median(y)
                stats[label] = (avg, var, med)
            info = u"{0}μ = {1:.3g}, σ = {2:.3g}, median = {3:.3g}"
            t = ax.text(0.75, 0.7,
                        '\n'.join([info.format(label + ': ' if len(stats) > 1 else '', avg, var, med)
                                   for label, (avg, var, med) in stats.items()]),
                        ha="center", va="center", transform=ax.transAxes, backgroundcolor='w')
            t.set_bbox({'color': 'w', 'alpha': .5, 'edgecolor': 'w'})

        if 'label' in kwargs or 'labels' in kwargs:
            ax.legend(bbox_to_anchor=(.5, .95),
                      frameon=False,
                      loc='lower center',
                      ncol=len(kwargs.get('label', kwargs.get('labels'))),
                      numpoints=1,
                      prop={'size': 7})

        mp_pickle(plotdir, filename, data)
        mp_saveimg(plotdir, filename)


def mp_saveimg(plotdir, name):
    logger.info("Saving {0}".format(name))

    plt.savefig(os.path.join(plotdir, name + '.png'))
    plt.savefig(os.path.join(plotdir, name + '.pdf'))

    plt.close()


class Plotter(object):
    TIME = 1
    HIST = 2
    PLOT = 4
    PROF = 8
    STACK = 16

    def __init__(self, config, outdir=None, paper=False):
        self.config = config
        self.__paper = paper
        self.__store = unit.UnitStore(config)

        util.verify(self.config.workdir)

        if outdir:
            self.__plotdir = outdir
        else:
            self.__plotdir = config.plotdir if config.plotdir else self.config.label
        self.__plotdir = os.path.expandvars(os.path.expanduser(self.__plotdir))

        if not os.path.isdir(self.__plotdir):
            os.makedirs(self.__plotdir)

    def parsetime(self, time):
        if not time:
            return None

        try:
            t = datetime.combine(
                datetime.today().date(),
                datetime.strptime(time, '%H:%M').timetz()
            )
            return int(t.strftime('%s'))
        except ValueError:
            pass

        try:
            t = datetime.strptime(time, '%Y-%m-%d_%H:%M')
            return int(t.strftime('%s'))
        except ValueError:
            pass

        t = datetime.strptime(time, '%Y-%m-%d')
        return int(t.strftime('%s'))

    def readdb(self):
        logger.debug('reading database')
        db = sqlite3.connect(os.path.join(
            self.config.workdir, 'lobster.db'), timeout=90)

        self.wflow_ids = {}
        self.wflow_labels = {}
        wflow_cores = {}
        for id_, label in db.execute("select id, label from workflows"):
            self.wflow_ids[label] = id_
            self.wflow_labels[id_] = label
            wflow_cores[id_] = getattr(
                self.config.workflows, label).category.cores

        cur = db.execute(
            "select * from tasks where time_retrieved>=? and time_retrieved<=?",
            (self.__xmin, self.__xmax))
        fields = [xs[0] for xs in cur.description]
        textfields = ['host', 'published_file_block']
        formats = ['i4' if f not in textfields else 'a100' for f in fields]
        tasks = np.array(cur.fetchall(), dtype={
                         'names': fields, 'formats': formats})

        # cores = [wflow_cores[n] for n in tasks['workflow']]
        # tasks = rfn.append_fields(tasks, 'cores', data=cores, usemask=False)

        failed_tasks = tasks[tasks['status'] == 3] if len(
            tasks) > 0 else np.array([], tasks.dtype)
        success_tasks = tasks[np.in1d(tasks['status'], (2, 6, 7, 8))] if len(
            tasks) > 0 else np.array([], tasks.dtype)

        summary_data = list(self.__store.workflow_status())[1:]

        # for cases where units per task changes during run, get per-unit info
        total_units = 0
        start_units = 0
        completed_units = []
        units_processed = {}
        transfers = {}
        for (label,) in db.execute("select label from workflows"):
            total_units += db.execute(
                "select count(*) from units_{0}".format(label)).fetchone()[0]
            start_units += db.execute("""
                select count(*)
                from units_{0}, tasks
                where units_{0}.task == tasks.id
                    and (units_{0}.status=2 or units_{0}.status=6)
                    and time_retrieved<=?""".format(label), (self.__xmin,)).fetchone()[0]
            completed_units.append(np.array(db.execute("""
                select units_{0}.id, tasks.time_retrieved
                from units_{0}, tasks
                where units_{0}.task == tasks.id
                    and (units_{0}.status=2 or units_{0}.status=6)
                    and time_retrieved>=? and time_retrieved<=?""".format(label),
                                                       (self.__xmin, self.__xmax)).fetchall(),
                                            dtype=[('id', 'i4'), ('time_retrieved', 'i4')]))
            units_processed[label] = db.execute("""
                select units_{0}.run,
                units_{0}.lumi
                from units_{0}, tasks
                where units_{0}.task == tasks.id
                    and (units_{0}.status in (2, 6))""".format(label)).fetchall()
            transfers[label] = json.loads(db.execute("""
                select transfers
                from workflows
                where label=?""", (label,)).fetchone()[0])

        logger.debug('finished reading database')

        return success_tasks, failed_tasks, summary_data, np.concatenate(completed_units), total_units, total_units - start_units, units_processed, transfers

    def readlog(self, filename=None, category='all'):
        if filename:
            fn = filename
        else:
            fn = os.path.join(self.config.workdir,
                              'lobster_stats_{}.log'.format(category))

        with open(fn) as f:
            headers = dict(map(lambda (a, b): (b, a),
                               enumerate(f.readline()[1:].split())))
        stats = np.loadtxt(fn)

        # fix units of time
        stats[:, 0] /= 1e6

        for label in ['joined', 'removed', 'lost', 'idled_out', 'fast_aborted', 'blacklisted', 'released']:
            field = 'workers_{}'.format(label)
            stats[:, headers[field]] = np.maximum(stats[:, headers[field]] - np.roll(stats[:, headers[field]], 1, 0), 0)

        if not filename and category == 'all':
            self.__total_xmin = stats[0, 0]
            self.__total_xmax = stats[-1, 0]

            if not self.__xmin:
                self.__xmin = stats[0, 0]
            if not self.__xmax:
                self.__xmax = stats[-1, 0]

        return headers, stats[np.logical_and(stats[:, 0] >= self.__xmin, stats[:, 0] <= self.__xmax)]

    def savejsons(self, processed):
        jsondir = os.path.join(self.__plotdir, 'jsons')
        if not os.path.exists(jsondir):
            os.makedirs(jsondir)

        res = {}
        for label in processed:
            if not os.path.exists(os.path.join(self.__plotdir, jsondir)):
                os.makedirs(os.path.join(self.__plotdir, jsondir))
            units = LumiList(lumis=processed[label])
            units.writeJSON(os.path.join(
                jsondir, 'processed_{}.json'.format(label)))
            res[label] = [
                (os.path.join('jsons', 'processed_{}.json'.format(label)), 'processed')]

            published = os.path.join(
                self.config.workdir, label, 'published.json')
            if os.path.isfile(published):
                shutil.copy(published, os.path.join(
                    jsondir, 'published_{}.json'.format(label)))
                res[label] += [(os.path.join('jsons',
                                             'published_{}.json'.format(label)), 'published')]

        return res

    def savelogs(self, failed_tasks, samples=10):
        logdir = os.path.join(self.__plotdir, 'logs')
        work = []
        codes = {}

        for exit_code, tasks in zip(*split_by_column(failed_tasks[['id', 'exit_code']], 'exit_code')):
            if exit_code == 0:
                continue

            codes[exit_code] = [len(tasks), []]

            logger.info(
                "Copying sample logs for exit code {0}".format(exit_code))
            for id, e in list(tasks[-samples:]):
                try:
                    source = glob.glob(os.path.join(
                        self.config.workdir, '*', 'failed', util.id2dir(id)))[0]
                except IndexError:
                    continue

                s = os.path.join(source, 'task.log.gz')
                t = os.path.join(logdir, str(id) + '.log')
                if os.path.exists(s):
                    codes[exit_code][1].append(str(id))
                    work.append([s, t])

        for label, _, _, _, _, _, _, _, _, failed, skipped, _, _, _ in list(self.__store.workflow_status())[1:-1]:
            if failed + skipped == 0:
                continue

            failed = self.__store.failed_units(label)
            skipped = self.__store.skipped_files(label)

            for id in failed:
                source = os.path.join(
                    self.config.workdir, label, 'failed', util.id2dir(id))
                target = os.path.join(logdir, 'failed_' + label)
                if not os.path.exists(target):
                    os.makedirs(target)

                for l in ['task.log.gz']:
                    s = os.path.join(source, l)
                    t = os.path.join(target, str(id) + "_" + l[:-3])
                    if os.path.exists(s):
                        work.append([s, t])

            if len(skipped) > 0:
                outname = os.path.join(logdir, 'skipped_{}.txt'.format(label))
                if not os.path.isdir(os.path.dirname(outname)):
                    os.makedirs(os.path.dirname(outname))
                with open(outname, 'w') as f:
                    f.write('\n'.join(skipped))
        pool = multiprocessing.Pool(10, reset_signals)
        pool.map(unpack, work)
        pool.close()
        pool.join()

        for code in codes:
            for id in range(samples - len(codes[code][1])):
                codes[code][1].insert(0, "")

        return codes

    def updatecpu(self, tasks, edges):
        cputime = np.zeros(len(edges) - 1)

        ratio = tasks['time_cpu'] * 1. / \
            (tasks['time_processing_end'] - tasks['time_prologue_end'])

        starts = np.digitize(tasks['time_prologue_end'], edges)
        ends = np.digitize(tasks['time_processing_end'], edges)

        lefts = np.take(edges, starts) - tasks['time_prologue_end']
        rights = tasks['time_processing_end'] - np.take(edges, ends - 1)

        for fraction, left, start, end, right in zip(ratio, lefts, starts, ends, rights):
            if start == end and start > 0 and start <= len(cputime):
                # do some special logic if the task is completely in one
                # bin: length = left - (bin width - right)
                cputime[start - 1] += fraction * \
                    (left + right - (edges[start] - edges[start - 1]))
            else:
                if start > 0 and start <= len(cputime):
                    cputime[start - 1] += fraction * left
                cputime[start:end - 1] += fraction * 60
                if end >= 0 and end < len(cputime):
                    cputime[end] += fraction * right

        return cputime

    def plot(self, a, xlabel, stub=None, ylabel='tasks', bins=50, modes=None, **kwargs_raw):
        args = [a, xlabel]
        kwargs = {
            'stub': stub,
            'ylabel': ylabel,
            'bins': bins,
            'modes': modes,
            'xmin': self.__xmin,
            'xmax': self.__xmax,
            'plotdir': self.__plotdir,
            'paper': self.__paper
        }
        kwargs.update(kwargs_raw)
        self.__plotargs.append((mp_plot, args, kwargs))

    def pie(self, vals, labels, name, **kwargs_raw):
        kwargs = {'plotdir': self.__plotdir, 'paper': self.__paper}
        kwargs.update(kwargs_raw)
        self.__plotargs.append((mp_pie, [vals, labels, name], kwargs))

    def make_foreman_plots(self):
        tasks = []
        idleness = []
        efficiencies = []

        names = []

        for filename in self.__foremen:
            headers, stats = self.readlog(filename)

            foreman = os.path.basename(filename)

            if re.match('.*log+', foreman):
                foreman = foreman[:foreman.rfind('.')]
                foreman = string.strip(foreman)
            names.append(foreman)

            tasks.append((stats[:, headers['timestamp']],
                          stats[:, headers['tasks_running']]))
            idleness.append((stats[:, headers['timestamp']], stats[
                            :, headers['idle_percentage']]))
            efficiencies.append(
                (stats[:, headers['timestamp']], stats[:, headers['efficiency']]))

            self.plot(
                [
                    (stats[:, headers['timestamp']],
                     stats[:, headers['workers_busy']]),
                    (stats[:, headers['timestamp']],
                     stats[:, headers['workers_idle']]),
                    (stats[:, headers['timestamp']], stats[
                        :, headers['workers_connected']])
                ],
                'Workers', foreman + '-workers',
                modes=[Plotter.PLOT | Plotter.TIME],
                label=['busy', 'idle', 'connected']
            )

            self.plot(
                [
                    (stats[:, headers['timestamp']],
                     stats[:, headers['workers_joined']]),
                    (stats[:, headers['timestamp']],
                     stats[:, headers['workers_removed']])
                ],
                'Workers', foreman + '-turnover',
                modes=[Plotter.HIST | Plotter.TIME],
                label=['joined', 'removed']
            )

            self.pie(
                [
                    np.sum(stats[:, headers['time_workers_execute_good']]),
                    np.sum(stats[:, headers['time_workers_execute']]) -
                    np.sum(stats[:, headers['time_workers_execute_good']])
                ],
                ["good execute time", "total-good execute time"],
                foreman + "-time-pie",
                colors=["green", "red"]
            )

        if len(names) == 0:
            return names

        self.plot(
            tasks,
            'Tasks', 'foreman-tasks',
            modes=[Plotter.PLOT | Plotter.TIME],
            label=names
        )

        self.plot(
            idleness,
            'Idle', 'foreman-idle',
            modes=[Plotter.PLOT | Plotter.TIME],
            label=names
        )

        self.plot(
            efficiencies,
            'Efficiency', 'foreman-efficiency',
            modes=[Plotter.PLOT | Plotter.TIME],
            label=names
        )

        return names

    def find_failure_hosts(self, failed_tasks, samples=10):
        if len(failed_tasks) == 0:
            return []

        hosts, counts = np.unique(failed_tasks['host'], return_counts=True)
        ind = np.argpartition(counts, -1)[-samples:]
        ind = ind[np.argsort(counts[ind])]
        hosts = hosts[ind]
        counts = counts[ind]

        failures, errors = np.unique(failed_tasks[np.in1d(failed_tasks['host'], hosts)][
                                     'exit_code'], return_counts=True)
        ind = np.argpartition(errors, -1)[-samples:]
        ind = ind[np.argsort(errors[ind])]
        failures = failures[ind][::-1]
        table = [["All"] + list(failures)]
        for h, c in reversed(zip(hosts, counts)):
            host_tasks = failed_tasks[failed_tasks['host'] == h]
            table.append(
                [h, c] + [len(host_tasks[host_tasks['exit_code'] == f]) for f in failures])
        return table

    def merge_transfers(self, transfers, labels):
        res = defaultdict(lambda: Counter({
            'stage-in success': 0,
            'stageout success': 0,
            'stage-in failure': 0,
            'stageout failure': 0
        }))

        for label in labels:
            for protocol in transfers[label]:
                res[protocol].update(Counter(transfers[label][protocol]))

        return res

    def make_time_fraction_plot(self, category):
        headers, stats = self.__category_stats[category]

        wq_labels = [
            'time_send', 'time_receive', 'time_status_msgs',
            'time_internal', 'time_polling', 'time_application'
        ]
        lobster_labels = ['status', 'create', 'action', 'update', 'fetch', 'return']
        return_labels = ['dash', 'handler', 'updates', 'elk', 'transfers', 'cleanup', 'propagate', 'sqlite']

        times = stats[:, headers['timestamp']]
        centers = ((times + np.roll(times, 1, 0)) * 0.5)[1:]
        centers[0] = times[0]
        centers[-1] = times[-1]

        def diff(label):
            label = 'total_{}_time'.format(label) if 'time' not in label else label
            quant = stats[:, headers[label]]
            return (quant - np.roll(quant, 1, 0))[1:]

        wq_stats = dict((label, diff(label)) for label in wq_labels)
        lobster_stats = dict((label, diff(label)) for label in lobster_labels)
        return_stats = dict((label, diff('source_' + label)) for label in return_labels)

        time_diff = ((times - np.roll(times, 1, 0)) * 1e6)[1:]
        everything = np.sum(lobster_stats.values(), axis=0)
        other = time_diff - everything

        self.plot(
            [
                (centers, np.divide(lobster_stats[label], everything)) for label in lobster_labels
            ] + [
                (centers, np.divide(other, everything))
            ],
            'Lobster fraction', os.path.join(category, 'lobster-fraction'),
            modes=[Plotter.STACK | Plotter.TIME],
            labels=lobster_labels + ['other'],
            ymax=1.
        )

        # This is the odd one out, since WQ only provides us with an idle
        # fraction
        idle_total = np.multiply(
            stats[:, headers['timestamp']] - stats[0, headers['timestamp']],
            stats[:, headers['idle_percentage']]
        )
        wq_stats['idle'] = (idle_total - np.roll(idle_total, 1, 0))[1:]

        everything = np.sum(wq_stats.values(), axis=0)
        other = lobster_stats['fetch'] - everything

        self.plot(
            [
                (centers, np.divide(wq_stats[label], everything)) for label in wq_labels
            ] + [
                (centers, np.divide(other, everything))
            ],
            'WQ fraction', os.path.join(category, 'wq-fraction'),
            modes=[Plotter.STACK | Plotter.TIME],
            labels=[x.replace('time_', '').replace('_', ' ') for x in wq_labels] + ['other'],
            ymax=1.
        )

        everything = np.sum(return_stats.values(), axis=0)
        other = lobster_stats['return'] - everything

        self.plot(
            [
                (centers, np.divide(return_stats[label], everything)) for label in return_labels
            ] + [
                (centers, np.divide(other, everything))
            ],
            'Return fraction', os.path.join(category, 'return-fraction'),
            modes=[Plotter.STACK | Plotter.TIME],
            labels=[x.replace('_', ' ') for x in return_labels] + ['other'],
            ymax=1.
        )

    def make_master_plots(self, category, good_tasks, success_tasks):
        headers, stats = self.__category_stats[category]
        edges = np.histogram(stats[:, headers['timestamp']], bins=50)[1]

        self.make_time_fraction_plot(category)

        self.plot(
            [
                (stats[:, headers['timestamp']],
                 stats[:, headers['workers_busy']]),
                (stats[:, headers['timestamp']],
                 stats[:, headers['workers_idle']]),
                (stats[:, headers['timestamp']],
                 stats[:, headers['workers_init']]),
                (stats[:, headers['timestamp']],
                 stats[:, headers['workers_connected']]),
                (stats[:, headers['timestamp']],
                 stats[:, headers['workers_able']])
            ],
            'Workers', os.path.join(category, 'workers'),
            modes=[Plotter.PLOT | Plotter.TIME],
            label=['busy', 'idle', 'init', 'connected', 'able']
        )

        for resource, unit_ in (('cores', ''), ('memory', '/ MB'), ('disk', '/ MB')):
            scale = 1
            if unit_ == '/ MB' \
                    and max(stats[:, headers['total_' + resource]]) > 1000 \
                    and max(stats[:, headers['committed_' + resource]]) > 1000:
                scale = 1000
                unit_ = '/ GB'
            self.plot(
                [
                    (stats[:, headers['timestamp']], stats[
                     :, headers['total_' + resource]] / scale),
                    (stats[:, headers['timestamp']], stats[
                        :, headers['committed_' + resource]] / scale)
                ],
                '{} {}'.format(resource[0].upper() +
                               resource[1:], unit_).strip(),
                os.path.join(category, resource),
                modes=[Plotter.PLOT | Plotter.TIME],
                label=['total', 'committed']
            )

        self.plot(
            [(stats[:, headers['timestamp']], stats[:, headers['tasks_running']])],
            'Tasks', os.path.join(category, 'tasks'),
            modes=[Plotter.PLOT | Plotter.TIME],
            label=['running']
        )

        self.plot(
            [
                (stats[:, headers['timestamp']],
                 stats[:, headers['workers_joined']]),
                (stats[:, headers['timestamp']],
                 stats[:, headers['workers_removed']])
            ],
            'Workers', os.path.join(category, 'turnover'),
            modes=[Plotter.HIST | Plotter.TIME],
            label=['joined', 'removed']
        )

        self.plot(
            [
                (stats[:, headers['timestamp']],
                 stats[:, headers['workers_lost']]),
                (stats[:, headers['timestamp']], stats[
                    :, headers['workers_idled_out']]),
                (stats[:, headers['timestamp']], stats[
                    :, headers['workers_fast_aborted']]),
                (stats[:, headers['timestamp']], stats[
                    :, headers['workers_blacklisted']]),
                (stats[:, headers['timestamp']], stats[
                    :, headers['workers_released']]),
            ],
            'Workers', os.path.join(category, 'worker-deaths'),
            modes=[Plotter.HIST | Plotter.TIME],
            label=['evicted', 'idled out',
                   'fast aborted', 'blacklisted', 'released']
        )

        if len(good_tasks) > 0:
            def integrate_wall(q):
                def integrate((x, y)):
                    indices = np.logical_and(stats[:, 0] >= x, stats[:, 0] < y)
                    values = stats[indices, headers[q]]
                    if len(values) > 0:
                        return np.sum(values) * (y - x) / len(values)
                    return 0
                return integrate

            walltime = np.array(map(integrate_wall('committed_cores'), zip(edges[:-1], edges[1:])))
            cputime = self.updatecpu(success_tasks, edges)

            centers = [(x + y) / 2 for x, y in zip(edges[:-1], edges[1:])]

            cputime[walltime == 0] = 0.
            walltime[walltime == 0] = 1e-6

            ratio = np.nan_to_num(np.divide(cputime * 1.0, walltime))

            self.plot(
                [(centers, ratio)],
                'CPU / Wall', os.path.join(category, 'cpu-wall'),
                bins=50,
                modes=[Plotter.HIST | Plotter.TIME]
            )

            ratio = np.nan_to_num(np.divide(np.cumsum(cputime) * 1.0, np.cumsum(walltime)))

            self.plot(
                [(centers, ratio)],
                'Integrated CPU / Wall', os.path.join(
                    category, 'cpu-wall-int'),
                bins=50,
                modes=[Plotter.HIST | Plotter.TIME]
            )

            walltime = np.array(map(integrate_wall('total_cores'), zip(edges[:-1], edges[1:])))
            walltime[walltime == 0] = 1e-6

            ratio = np.nan_to_num(np.divide(cputime * 1.0, walltime))

            self.plot(
                [(centers, ratio)],
                'CPU / Coretime', os.path.join(category, 'cpu-cores'),
                bins=50,
                modes=[Plotter.HIST | Plotter.TIME]
            )

            ratio = np.nan_to_num(np.divide(np.cumsum(cputime) * 1.0, np.cumsum(walltime)))

            self.plot(
                [(centers, ratio)],
                'Integrated CPU / Coretime', os.path.join(category, 'cpu-cores-int'),
                bins=50,
                modes=[Plotter.HIST | Plotter.TIME]
            )

        return edges

    def make_workflow_plots(self, subdir, edges, good_tasks, failed_tasks, success_tasks, merge_tasks, xmin=None, xmax=None):
        if len(good_tasks) > 0 or len(failed_tasks) > 0:
            headers, stats = self.__category_stats[subdir]
            dtime = stats[:, headers['timestamp']] - np.roll(stats[:, headers['timestamp']], 1, 0)
            dtime[dtime < 0] = 0.
            mcommitted = (stats[:, headers['committed_cores']] + np.roll(stats[:, headers['committed_cores']], 1, 0)) * .5
            mtotal = (stats[:, headers['total_cores']] + np.roll(stats[:, headers['total_cores']], 1, 0)) * .5

            self.pie(
                [
                    np.dot(dtime, mtotal - mcommitted),
                    np.dot(good_tasks['cores'], good_tasks['time_total_on_worker'] - good_tasks['time_on_worker']) +
                    np.dot(failed_tasks['cores'], failed_tasks['time_total_on_worker'] - failed_tasks['time_on_worker']),
                    (
                        np.dot(good_tasks['cores'], good_tasks['time_total_exhausted_execution']) +
                        np.dot(failed_tasks['cores'], failed_tasks['time_total_exhausted_execution'])
                    ),
                    np.dot(failed_tasks['cores'], failed_tasks['time_total_on_worker']),
                    np.dot(good_tasks['cores'], good_tasks['time_prologue_end'] - good_tasks['time_transfer_in_start']),
                    np.dot(good_tasks['cores'], good_tasks['time_processing_end'] - good_tasks['time_prologue_end']),
                    np.dot(good_tasks['cores'], good_tasks['time_transfer_out_end'] - good_tasks['time_processing_end'])
                ],
                ["Cores-idle", "Eviction", "Exhausted", "Failed", "Overhead", "Processing", "Stage-out"],
                os.path.join(subdir, "time-pie"),
                colors=["maroon", "crimson", "coral", "red", "dodgerblue", "green", "skyblue"]
            )

            workflows = []
            colors = []
            labels = []

            for tasks, label, success_color, merged_color, merging_color in [
                    (success_tasks, 'processing',
                     'green', 'lightgreen', 'darkgreen'),
                    (merge_tasks, 'merging', 'purple', 'fuchsia', 'darkorchid')]:
                code_map = {
                    2: (label + ' (status: successful)', success_color),
                    6: ('published', 'blue'),
                    7: (label + ' (status: merging)', merging_color),
                    8: (label + ' (status: merged)', merged_color)
                }
                codes, split_tasks = split_by_column(tasks, 'status')

                workflows += [(x['time_retrieved'], [1] *
                               len(x['time_retrieved'])) for x in split_tasks]
                colors += [code_map[code][1] for code in codes]
                labels += [code_map[code][0] for code in codes]

            if len(failed_tasks) > 0:
                workflows += [(x['time_retrieved'], [1] *
                               len(x['time_retrieved'])) for x in [failed_tasks]]
                colors += ['red']
                labels += ['failed']

            self.plot(
                workflows,
                'tasks', os.path.join(subdir, 'all-tasks'),
                modes=[Plotter.HIST | Plotter.TIME],
                label=labels,
                color=colors
            )

        if len(good_tasks) > 0:
            output, bins = np.histogram(
                success_tasks['time_retrieved'], 100,
                weights=success_tasks['bytes_output'] / 1024.0 ** 3
            )

            total_output = np.cumsum(output)
            centers = [(x + y) / 2 for x, y in zip(bins[:-1], bins[1:])]

            scale = 3600.0 / ((bins[1] - bins[0]) * 1024.0 ** 3)

            self.plot(
                [(success_tasks['time_retrieved'],
                  success_tasks['bytes_output'] * scale)],
                'Output / (GB/h)', os.path.join(subdir, 'output'),
                bins=50,
                modes=[Plotter.HIST | Plotter.TIME]
            )

            self.plot(
                [(centers, total_output)],
                'Output / GB', os.path.join(subdir, 'output-total'),
                bins=50,
                modes=[Plotter.PLOT | Plotter.TIME]
            )

            for prefix, tasks in [('good-', success_tasks), ('merge-', merge_tasks)]:
                if len(tasks) == 0:
                    continue

                cache_map = {0: ('cold cache', 'lightskyblue'), 1: (
                    'hot cache', 'navy'), 2: ('dedicated', 'darkorchid')}
                cache, split_tasks = split_by_column(tasks, 'cache')
                # plot timeline
                things_we_are_looking_at = [
                    # x-times              , y-times
                    # , y-label                      , filestub             ,
                    # color            , in pie
                    ([(x['time_wrapper_start'], x['time_total_until_worker_failure'])
                      for x in split_tasks], 'Eviction', 'eviction', "crimson", False),  # red
                    ([(x['time_wrapper_start'], x['time_total_exhausted_execution'])
                      for x in split_tasks], 'Exhausted resources', 'exhaustion', "coral", False),  # orange
                    ([(x['time_wrapper_start'], x['time_processing_end'] - x['time_wrapper_start'])
                      for x in split_tasks], 'Runtime', 'runtime', "green", False),  # red
                    ([(x['time_wrapper_start'], x['time_transfer_in_end'] - x['time_transfer_in_start'])
                      for x in split_tasks], 'Input transfer', 'transfer-in', "black", True),  # gray
                    ([(x['time_wrapper_start'], x['time_wrapper_start'] - x['time_transfer_in_end'])
                      for x in split_tasks], 'Startup', 'startup', "darkorchid", True),  # blue
                    ([(x['time_wrapper_start'], x['time_wrapper_ready'] - x['time_wrapper_start'])
                      for x in split_tasks], 'Release setup', 'setup-release', "navy", True),  # blue
                    ([(x['time_wrapper_start'], x['time_stage_in_end'] - x['time_wrapper_ready'])
                      for x in split_tasks], 'Stage-in', 'stage-in', "gray", True),  # gray
                    ([(x['time_wrapper_start'], x['time_prologue_end'] - x['time_stage_in_end'])
                      for x in split_tasks], 'Prologue', 'prologue', "orange", True),  # yellow
                    ([(x['time_wrapper_start'], x['time_wrapper_ready'] - x['time_wrapper_start'])
                      for x in split_tasks], 'Overhead', 'overhead', "blue", False),  # blue]
                    ([(x['time_wrapper_start'], x['time_processing_end'] - x['time_prologue_end'])
                      for x in split_tasks], 'Executable', 'processing', "forestgreen", True),  # green
                    ([(x['time_wrapper_start'], x['time_epilogue_end'] - x['time_processing_end'])
                      for x in split_tasks], 'Epilogue', 'epilogue', "khaki", True),  # yellow
                    ([(x['time_wrapper_start'], x['time_stage_out_end'] - x['time_epilogue_end'])
                      for x in split_tasks], 'Stage-out', 'stage-out', "silver", True),  # gray
                    ([(x['time_wrapper_start'], x['time_transfer_out_start'] - x['time_stage_out_end'])
                      for x in split_tasks], 'Output transfer wait', 'transfer-out-wait', "lightskyblue", True),  # blue
                    ([(x['time_wrapper_start'], x['time_transfer_out_end'] - x['time_transfer_out_start'])
                      for x in split_tasks], 'Output transfer work_queue', 'transfer-out-wq', "gainsboro", True)    # gray
                ]

                times_by_cache = [plot[0]
                                  for plot in things_we_are_looking_at if plot[-1]]
                self.pie(
                    [np.sum([np.sum(x[1]) for x in times])
                     for times in times_by_cache],
                    [plot[1] for plot in things_we_are_looking_at if plot[-1]],
                    os.path.join(subdir, prefix + "time-detail-pie"),
                    colors=[plot[-2]
                            for plot in things_we_are_looking_at if plot[-1]]
                )

                for a, label, filestub, color, pie in things_we_are_looking_at:
                    self.plot(
                        [(xtimes, ytimes / 60.) for xtimes, ytimes in a],
                        label +
                        ' / m', os.path.join(subdir, prefix + filestub),
                        color=[cache_map[x][1] for x in cache],
                        label=[cache_map[x][0] for x in cache]
                    )

                self.plot(
                    [(tasks['time_retrieved'], tasks['cores'])],
                    'cores', os.path.join(subdir, prefix + 'cores'),
                )

                self.plot(
                    [
                        (tasks['time_retrieved'], tasks['memory_resident']),
                        (tasks['time_retrieved'], tasks['memory_virtual']),
                        (tasks['time_retrieved'], tasks['memory_swap'])
                    ],
                    'memory / MB', os.path.join(subdir, prefix + 'memory'),
                    label=['resident', 'virtual', 'swap']
                )

                self.plot(
                    [(tasks['time_retrieved'], tasks['workdir_footprint'])],
                    'working directory footprint / MB', os.path.join(
                        subdir, prefix + 'workdir-footprint'),
                )

                bandwidth = tasks['network_bytes_received'] * 8 / \
                    1e6 / tasks['time_on_worker']
                self.plot(
                    [(tasks['time_retrieved'], bandwidth)],
                    'bandwidth / Mb/s', os.path.join(subdir,
                                                     prefix + 'network-bandwidth'),
                    modes=[Plotter.PROF | Plotter.TIME]
                )

                efficiency = tasks['time_cpu'] / (1. * tasks['cores'] * (
                    tasks['time_processing_end'] - tasks['time_prologue_end']))
                self.plot(
                    [(tasks['time_retrieved'], efficiency)],
                    'Executable CPU/Wall time', os.path.join(
                        subdir, prefix + 'exe-efficiency'),
                    modes=[Plotter.HIST]
                )

                for resource, unit_ in (('cores', ''), ('disk', '/ MB'), ('memory', '/ MB')):
                    self.plot(
                        [(tasks['time_transfer_in_start'],
                          tasks['allocated_' + resource])],
                        'allocated {} {}'.format(resource, unit_).strip(),
                        os.path.join(subdir, prefix + 'allocated-' + resource),
                        modes=[Plotter.PROF | Plotter.TIME]
                    )

                wflows, wtasks = split_by_column(tasks, 'workflow')
                self.plot(
                    [(t['time_submit'], t['units']) for t in wtasks],
                    'task size / units', os.path.join(subdir,
                                                      prefix + 'tasksize'),
                    modes=[Plotter.PROF | Plotter.TIME],
                    label=[self.wflow_labels[w] for w in wflows]
                )

                self.plot(
                    [(tasks['time_retrieved'], tasks['exhausted_attempts'])],
                    'exhausted attempts', os.path.join(
                        subdir, prefix + 'exhausted-attempts'),
                )

        if len(failed_tasks) > 0:
            logs = self.savelogs(failed_tasks)

            fail_labels, fail_values = split_by_column(
                failed_tasks, 'exit_code', threshold=0.025)

            self.pie(
                [len(xs['time_retrieved']) for xs in fail_values],
                fail_labels,
                os.path.join(subdir, "failed-pie")
            )

            self.plot(
                [(xs['time_retrieved'], [1] * len(xs['time_retrieved']))
                 for xs in fail_values],
                'Failed tasks', os.path.join(subdir, 'failed-tasks'),
                modes=[Plotter.HIST | Plotter.TIME],
                label=map(str, fail_labels)
            )

            self.plot(
                [(tasks['time_retrieved'], tasks['cores'])],
                'cores', os.path.join(subdir, 'failed-cores'),
            )

            self.plot(
                [
                    (failed_tasks['time_retrieved'],
                     failed_tasks['memory_resident']),
                    (failed_tasks['time_retrieved'],
                     failed_tasks['memory_virtual']),
                    (failed_tasks['time_retrieved'],
                     failed_tasks['memory_swap'])
                ],
                'memory / MB', os.path.join(subdir, 'failed-memory'),
                label=['resident', 'virtual', 'swap']
            )

            self.plot(
                [(failed_tasks['time_retrieved'], failed_tasks['workdir_footprint'])],
                'working directory footprint / MB', os.path.join(
                    subdir, 'failed-workdir-footprint'),
            )

            self.plot(
                [(failed_tasks['time_retrieved'], failed_tasks['exhausted_attempts'])],
                'exhausted attempts', os.path.join(
                    subdir, 'failed-exhausted-attempts'),
            )

        else:
            logs = None

        return logs

    def make_plots(self, xmin=None, xmax=None, foremen=None):
        self.__plotargs = []
        self.__xmin = self.parsetime(xmin)
        self.__xmax = self.parsetime(xmax)

        self.__foremen = foremen if foremen else []

        # readlog() determines the time bounds of sql queries if not
        # specified explicitly.
        self.__category_stats = {'all': self.readlog()}
        for category in self.config.categories:
            label = category.name
            if label == 'merge':
                continue
            self.__category_stats[label] = self.readlog(category=label)

        good_tasks, failed_tasks, summary_data, completed_units, total_units, start_units, units_processed, transfers = self.readdb()

        success_tasks = good_tasks[good_tasks['type'] == 0] if len(
            good_tasks) > 0 else np.array([], good_tasks.dtype)
        merge_tasks = good_tasks[good_tasks['type'] == 1] if len(
            good_tasks) > 0 else np.array([], good_tasks.dtype)

        # -------------
        # General plots
        # -------------
        foremen_names = self.make_foreman_plots()

        if len(good_tasks) > 0:
            completed, bins = np.histogram(
                completed_units['time_retrieved'], 100)
            total_completed = np.cumsum(completed)
            centers = [(x + y) / 2 for x, y in zip(bins[:-1], bins[1:])]

            self.plot(
                [(centers, total_completed * (-1.) + start_units)],
                'units remaining', 'units-total',
                bins=50,
                modes=[Plotter.PLOT | Plotter.TIME]
            )

            data = []
            labels = []
            for label, (headers, stats) in self.__category_stats.items():
                data.append((stats[:, headers['timestamp']],
                             stats[:, headers['tasks_running']]))
                labels.append(label)

            self.plot(
                data, 'Tasks running', 'tasks',
                modes=[Plotter.PLOT | Plotter.TIME],
                label=labels
            )

        # ----------
        # Templating
        # ----------

        categories = [
            c.name for c in self.config.categories if c.name != 'merge']
        category_summary_data = []

        def date2string(d):
            return datetime.fromtimestamp(d).strftime('%a, %d %b %Y, %H:%M')

        def istotal(s):
            return s == "Total"

        env = jinja2.Environment(loader=jinja2.FileSystemLoader(
            os.path.join(os.path.dirname(__file__), 'data')))
        env.filters["datetime"] = date2string
        env.tests["sum"] = istotal
        overview = env.get_template('index.html')
        wflow = env.get_template('category.html')

        jsons = self.savejsons(units_processed)

        shutil.copy(os.path.join(self.config.workdir, 'config.py'),
                    os.path.join(self.__plotdir, 'config.py'))
        for fn in ['styles.css', 'gh.png']:
            shutil.copy(os.path.join(os.path.dirname(__file__), 'data', fn),
                        os.path.join(self.__plotdir, fn))

        # -----------------------
        # Category specific plots
        # -----------------------
        logdir = os.path.join(self.__plotdir, 'logs')
        if os.path.exists(logdir):
            for dirpath, dirnames, filenames in os.walk(logdir):
                logs = [os.path.join(dirpath, fn)
                        for fn in filenames if fn.endswith('.log')]
                map(os.unlink, logs)
        else:
            os.makedirs(logdir)

        outdir = os.path.join(self.__plotdir, 'all')
        if not os.path.exists(outdir):
            os.makedirs(outdir)
        edges = self.make_master_plots('all', good_tasks, success_tasks)
        logs = self.make_workflow_plots(
            'all', edges, good_tasks, failed_tasks, success_tasks, merge_tasks, xmin, xmax)

        labels = [w.label for w in self.config.workflows]

        with open(os.path.join(self.__plotdir, 'all', 'index.html'), 'w') as f:
            f.write(wflow.render(
                id=self.config.label,
                label='all workflows',
                bad_tasks=len(failed_tasks) > 0,
                good_tasks=len(success_tasks) > 0,
                merge_tasks=len(merge_tasks) > 0,
                summary=summary_data,
                jsons=jsons,
                bad_logs=logs,
                bad_hosts=self.find_failure_hosts(failed_tasks),
                foremen=foremen_names,
                categories=categories,
                transfers=self.merge_transfers(transfers, labels)
            ).encode('utf-8'))

        def add_total(summaries):
            numbers = zip(*[s[1:-2] for s in summaries])
            total = map(sum, numbers)
            total_mergeable = sum([s[-8] for s in summaries if
                                   getattr(self.config.workflows, s[0], None) and
                                   getattr(self.config.workflows, s[0]).merge_size > 0])
            return summaries + \
                [['Total'] + total + [
                    '{} %'.format(round(total[-5] * 100. / total[-6], 1)),
                    '{} %'.format(round(total[-4] * 100. / total_mergeable if total_mergeable > 0 else 0, 1))
                ]]

        for category in self.config.categories:
            label = category.name
            if label == 'merge':
                continue
            ids = []
            labels = []
            for workflow in self.config.workflows:
                if workflow.category == category:
                    ids.append(self.wflow_ids[workflow.label])
                    labels.append(workflow.label)

            outdir = os.path.join(self.__plotdir, label)
            if not os.path.exists(outdir):
                os.makedirs(outdir)

            wf_good_tasks = good_tasks[np.in1d(good_tasks['workflow'], ids)]
            wf_failed_tasks = failed_tasks[
                np.in1d(failed_tasks['workflow'], ids)]
            wf_success_tasks = success_tasks[
                np.in1d(success_tasks['workflow'], ids)]
            wf_merge_tasks = merge_tasks[np.in1d(merge_tasks['workflow'], ids)]

            self.make_master_plots(label, wf_good_tasks, wf_success_tasks)
            logs = self.make_workflow_plots(label, edges,
                                            wf_good_tasks,
                                            wf_failed_tasks,
                                            wf_success_tasks,
                                            wf_merge_tasks,
                                            xmin, xmax)

            summary = add_total([xs for xs in summary_data if xs[0] in labels])
            category_summary_data.append([label] + summary[-1][1:])

            with open(os.path.join(self.__plotdir, label, 'index.html'), 'w') as f:
                f.write(wflow.render(
                    id=self.config.label,
                    label=label,
                    bad_tasks=len(wf_failed_tasks) > 0,
                    good_tasks=len(wf_success_tasks) > 0,
                    merge_tasks=len(wf_merge_tasks) > 0,
                    summary=summary,
                    jsons=jsons,
                    bad_logs=logs,
                    bad_hosts=self.find_failure_hosts(wf_failed_tasks),
                    foremen=foremen_names,
                    categories=categories,
                    transfers=self.merge_transfers(transfers, labels)
                ).encode('utf-8'))

        # Add the total from the unit store query
        category_summary_data.append(summary_data[-1])
        with open(os.path.join(self.__plotdir, 'index.html'), 'w') as f:
            f.write(overview.render(
                id=self.config.label,
                plot_time=time.time(),
                plot_starttime=self.__xmin,
                plot_endtime=self.__xmax,
                run_starttime=self.__total_xmin,
                run_endtime=self.__total_xmax,
                summary=category_summary_data,
                bad_tasks=len(failed_tasks) > 0,
                good_tasks=len(success_tasks) > 0,
                foremen=foremen_names,
                categories=categories
            ).encode('utf-8'))

        p = multiprocessing.Pool(10, reset_signals)
        p.map(mp_call, self.__plotargs)
        p.close()
        p.join()


class Plot(Command):

    @property
    def help(self):
        return "plot progess of processing"

    def setup(self, argparser):
        argparser.add_argument("--from", default=None, metavar="START", dest="xmin",
                               help="plot data from START.  Valid values: 1970-01-30, 1970-01-30_00:00, 00:00")
        argparser.add_argument("--to", default=None, metavar="END", dest="xmax",
                               help="plot data until END.  Valid values: 1970-01-30, 1970-01-30_00:00, 00:00")
        argparser.add_argument("--foreman-logs", default=None, metavar="FOREMAN_LIST", dest="foreman_list", nargs='+', type=str,
                               help="specify log files for foremen;  valid values: log1 log2 log3...logN")
        argparser.add_argument('--paper', action='store_true', default=False,
                               help="use black & white style for publications")
        argparser.add_argument('--outdir', help="specify output directory")

    def run(self, args):
        p = Plotter(args.config, args.outdir, args.paper)
        p.make_plots(args.xmin, args.xmax, args.foreman_list)
