# vim: fileencoding=utf-8

from datetime import datetime
import glob
import gzip
import jinja2
import logging
import multiprocessing
import os
import pickle
import shutil
import sqlite3
import time
import yaml
import re
import string

import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as dates
import numpy as np

from lobster import util
from lobster.cmssw import jobit

from FWCore.PythonUtilities.LumiList import LumiList

matplotlib.rc('axes', labelsize='large')
matplotlib.rc('figure', figsize=(8, 1.5))
matplotlib.rc('figure.subplot', left=0.09, right=0.92, bottom=0.275)
matplotlib.rc('font', size=7)
matplotlib.rc('font', **{'sans-serif' : 'Liberation Sans', 'family' : 'sans-serif'})

logger = logging.getLogger('lobster.plotting')

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

def unpack(source, target):
    try:
        logger.info("unpacking {0}".format(source))
        with open(target, 'w') as output:
            input = gzip.open(source, 'rb')
            output.writelines(input)
            input.close()
    except IOError:
        logger.error("cannot unpack {0}".format(source))
        return False
    return True

class Plotter(object):
    TIME = 1
    HIST = 2
    PLOT = 4
    PROF = 8

    def __init__(self, config, outdir=None):
        self.__workdir = config['workdir']
        self.__store = jobit.JobitStore(config)

        util.verify(self.__workdir)
        self.__id = config['id']

        if outdir:
            self.__plotdir = outdir
        else:
            self.__plotdir = config.get("plotdir", self.__id)
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
        db = sqlite3.connect(os.path.join(self.__workdir, 'lobster.db'))
        stats = {}

        failed_jobs = np.array(db.execute("""
            select
                id,
                host,
                dataset,
                exit_code,
                time_submit,
                time_retrieved,
                time_on_worker,
                time_total_on_worker,
                type
            from jobs
            where status=3 and time_retrieved>=? and time_retrieved<=?""",
            (self.__xmin, self.__xmax)).fetchall(),
                dtype=[
                    ('id', 'i4'),
                    ('host', 'a50'),
                    ('dataset', 'i4'),
                    ('exit_code', 'i4'),
                    ('t_submit', 'i4'),
                    ('t_retrieved', 'i4'),
                    ('t_goodput', 'i8'),
                    ('t_allput', 'i8'),
                    ('type', 'i4')
                    ])

        success_jobs = np.array(db.execute("""
            select
            id,
            host,
            dataset,
            status,
            exit_code,
            submissions,
            jobits,
            jobits_processed,
            events_read,
            events_written,
            time_submit,
            time_transfer_in_start,
            time_transfer_in_end,
            time_wrapper_start,
            time_wrapper_ready,
            time_stage_in_end,
            time_prologue_end,
            ifnull(time_file_requested, time_prologue_end),
            ifnull(time_file_opened, time_prologue_end),
            ifnull(time_file_processing, time_prologue_end),
            time_processing_end,
            time_epilogue_end,
            time_stage_out_end,
            time_transfer_out_start,
            time_transfer_out_end,
            time_retrieved,
            time_on_worker,
            time_total_on_worker,
            time_cpu,
            bytes_received,
            bytes_sent,
            bytes_output,
            type,
            cache
            from jobs
            where status in (2, 6, 7, 8) and time_retrieved>=? and time_retrieved<=?""",
            (self.__xmin, self.__xmax)).fetchall(),
                dtype=[
                    ('id', 'i4'),
                    ('host', 'a50'),
                    ('dataset', 'i4'),
                    ('status', 'i4'),
                    ('exit_code', 'i4'),
                    ('retries', 'i4'),
                    ('lumis', 'i4'),
                    ('processed_lumis', 'i4'),
                    ('events_r', 'i4'),
                    ('events_w', 'i4'),
                    ('t_submit', 'i4'),
                    ('t_send_start', 'i4'),
                    ('t_send_end', 'i4'),
                    ('t_wrapper_start', 'i4'),
                    ('t_wrapper_ready', 'i4'),
                    ('t_stage_in', 'i4'),
                    ('t_prologue', 'i4'),
                    ('t_file_req', 'i4'),
                    ('t_file_open', 'i4'),
                    ('t_first_ev', 'i4'),
                    ('t_processing_end', 'i4'),
                    ('t_epilogue', 'i4'),
                    ('t_stage_out', 'i4'),
                    ('t_recv_start', 'i4'),
                    ('t_recv_end', 'i4'),
                    ('t_retrieved', 'i4'),
                    ('t_goodput', 'i8'),
                    ('t_allput', 'i8'),
                    ('t_cpu', 'i8'),
                    ('b_recv', 'i4'),
                    ('b_sent', 'i4'),
                    ('b_output', 'i4'),
                    ('type', 'i4'),
                    ('cache', 'i4')
                    ])

        summary_data = list(db.execute("""
                select
                    label,
                    events,
                    (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                    (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                    jobits + masked_lumis,
                    jobits,
                    jobits_done,
                    jobits_paused,
                    '' || round(
                            jobits_done * 100.0 / jobits,
                        1) || ' %'
                from datasets"""))
        summary_data += list(db.execute("""
                select
                    'Total',
                    sum(events),
                    (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0),
                    (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0),
                    sum(jobits + masked_lumis),
                    sum(jobits),
                    sum(jobits_done),
                    sum(jobits_paused),
                    '' || round(
                            sum(jobits_done) * 100.0 / sum(jobits),
                        1) || ' %'
                from datasets"""))

        # for cases where jobits per job changes during run, get per-jobit info
        total_jobits = 0
        start_jobits = 0
        completed_jobits = []
        processed_lumis = {}
        for (label,) in db.execute("select label from datasets"):
            total_jobits += db.execute("select count(*) from jobits_{0}".format(label)).fetchone()[0]
            start_jobits += db.execute("""
                select count(*)
                from jobits_{0}, jobs
                where jobits_{0}.job == jobs.id
                    and (jobits_{0}.status=2 or jobits_{0}.status=6)
                    and time_retrieved<=?""".format(label), (self.__xmin,)).fetchone()[0]
            completed_jobits.append(np.array(db.execute("""
                select jobits_{0}.id, jobs.time_retrieved
                from jobits_{0}, jobs
                where jobits_{0}.job == jobs.id
                    and (jobits_{0}.status=2 or jobits_{0}.status=6)
                    and time_retrieved>=? and time_retrieved<=?""".format(label),
                (self.__xmin, self.__xmax)).fetchall(),
                dtype=[('id', 'i4'), ('t_retrieved', 'i4')]))
            processed_lumis[label] = db.execute("""
                select jobits_{0}.run,
                jobits_{0}.lumi
                from jobits_{0}, jobs
                where jobits_{0}.job == jobs.id
                    and (jobits_{0}.status in (2, 6))""".format(label)).fetchall()

        return success_jobs, failed_jobs, summary_data, np.concatenate(completed_jobits), total_jobits, total_jobits - start_jobits, processed_lumis

    def readlog(self, filename=None):
        if filename:
            fn = filename
        else:
            fn = os.path.join(self.__workdir, 'lobster_stats.log')

        with open(fn) as f:
            headers = dict(map(lambda (a, b): (b, a), enumerate(f.readline()[1:].split())))
        stats = np.loadtxt(fn)

        diff = stats[:,0] - np.roll(stats[:,0], 1, 0)

        # fix units of time
        stats[:,0] /= 1e6

        stats[:,headers['total_workers_joined']] = np.maximum(stats[:,headers['total_workers_joined']] - np.roll(stats[:,headers['total_workers_joined']], 1, 0), 0)
        stats[:,headers['total_workers_removed']] = np.maximum(stats[:,headers['total_workers_removed']] - np.roll(stats[:,headers['total_workers_removed']], 1, 0), 0)

        if 'total_create_time' in headers:
            # these are attributes present in the lobster stats log, but
            # not wq logs
            stats[:,headers['total_create_time']] -= np.roll(stats[:,headers['total_create_time']], 1, 0)
            stats[:,headers['total_create_time']] /= 60e6
            stats[:,headers['total_return_time']] -= np.roll(stats[:,headers['total_return_time']], 1, 0)
            stats[:,headers['total_return_time']] /= 60e6

        stats[:,headers['total_send_time']] -= np.roll(stats[:,headers['total_send_time']], 1, 0)
        stats[:,headers['total_send_time']] /= 60e6
        stats[:,headers['total_receive_time']] -= np.roll(stats[:,headers['total_receive_time']], 1, 0)
        stats[:,headers['total_receive_time']] /= 60e6

        if not filename:
            self.__total_xmin = stats[0,0]
            self.__total_xmax = stats[-1,0]

            if not self.__xmin:
                self.__xmin = stats[0,0]
            if not self.__xmax:
                self.__xmax = stats[-1,0]

        return headers, stats[np.logical_and(stats[:,0] >= self.__xmin, stats[:,0] <= self.__xmax)]

    def savejsons(self, processed):
        jsondir = os.path.join(self.__plotdir, 'jsons')
        if not os.path.exists(jsondir):
            os.makedirs(jsondir)

        res = {}
        for label in processed:
            jsondir = os.path.join('jsons', label)
            if not os.path.exists(os.path.join(self.__plotdir, jsondir)):
                os.makedirs(os.path.join(self.__plotdir, jsondir))
            lumis = LumiList(lumis=processed[label])
            lumis.writeJSON(os.path.join(self.__plotdir, jsondir, 'processed.json'))
            res[label] = [(os.path.join(jsondir, 'processed.json'), 'processed')]

            published = os.path.join(self.__workdir, label, 'published.json')
            if os.path.isfile(published):
                shutil.copy(published, os.path.join(self.__plotdir, jsondir))
                res[label] += [(os.path.join(jsondir, 'published.json'), 'published')]

        return res

    def savelogs(self, failed_jobs, samples=5):
        pool = multiprocessing.Pool(processes=10)
        work = []
        codes = {}

        logdir = os.path.join(self.__plotdir, 'logs')
        if os.path.exists(logdir):
            shutil.rmtree(logdir)
        os.makedirs(logdir)

        for exit_code, jobs in zip(*split_by_column(failed_jobs[['id', 'exit_code']], 'exit_code')):
            codes[exit_code] = [len(jobs), {}]

            logger.info("Copying sample logs for exit code {0}".format(exit_code))
            for id, e in list(jobs[-samples:]):
                codes[exit_code][1][id] = []

                try:
                    source = glob.glob(os.path.join(self.__workdir, '*', 'failed', util.id2dir(id)))[0]
                except IndexError:
                    continue

                target = os.path.join(os.path.join(self.__plotdir, 'logs'), str(id))
                if os.path.exists(target):
                    shutil.rmtree(target)
                os.makedirs(target)

                files = []
                for l in ['executable.log.gz', 'job.log.gz']:
                    s = os.path.join(source, l)
                    t = os.path.join(target, l[:-3])
                    if os.path.exists(s):
                        codes[exit_code][1][id].append(l[:-3])
                        work.append((exit_code, id, l[:-3], pool.apply_async(unpack, [s, t])))
        for (code, id, file, res) in work:
            if not res.get():
                codes[code][1][id].remove(file)

        work = []
        for label, _, _, _, _, _, _, paused, _ in self.__store.dataset_status()[1:]:
            if paused == 0:
                continue

            failed = self.__store.failed_jobits(label)
            skipped = self.__store.skipped_files(label)

            for id in failed:
                source = os.path.join(self.__workdir, label, 'failed', util.id2dir(id))
                target = os.path.join(self.__plotdir, 'logs', label, 'failed')
                if os.path.exists(target):
                    shutil.rmtree(target)
                os.makedirs(target)

                files = []
                for l in ['executable.log.gz', 'job.log.gz']:
                    s = os.path.join(source, l)
                    t = os.path.join(target, str(id) + "_" + l[:-3])
                    if os.path.exists(s):
                        work.append(pool.apply_async(unpack, [s, t]))

            if len(skipped) > 0:
                outname = os.path.join(self.__plotdir, 'logs', label, 'skipped_files.txt')
                if not os.path.isdir(os.path.dirname(outname)):
                    os.makedirs(os.path.dirname(outname))
                with open(outname, 'w') as f:
                    f.write('\n'.join(skipped))
        for res in work:
            res.get()

        pool.close()
        pool.join()

        for code in codes:
            for id in range(samples - len(codes[code][1])):
                codes[code][1][-id] = []

        return codes

    def unix2matplotlib(self, time):
        return dates.date2num(datetime.fromtimestamp(time))

    def updatecpu(self, jobs, reshape):
        cache = os.path.join(self.__workdir, 'cputime.pkl')
        edges = np.arange(self.__xmin, self.__xmax + 60, 60)

        try:
            with open(cache, 'rb') as f:
                cputime, ids = pickle.load(f)
                cputime.resize(len(edges) - 1)
            logger.info("reusing previously calculated cpu time stats.")
        except:
            logger.warning("calculating cpu time split up from scratch.")
            cputime = np.zeros(len(edges) - 1)
            ids = set()

        for (id, cpu, start, end) in zip(jobs['id'], jobs['t_cpu'], jobs['t_first_ev'], jobs['t_processing_end']):
            if end == start or cpu == 0 or id in ids:
                continue

            ids.add(id)
            ratio = cpu * 1. / (end - start)
            wall = 0
            for i in range(len(edges) - 1):
                if start >= edges[i] and end < edges[i + 1]:
                    cputime[i] += (end - start) * ratio
                    wall += (end - start) * ratio
                elif start < edges[i] and end >= edges[i + 1]:
                    cputime[i] += (edges[i + 1] - edges[i]) * ratio
                    wall += (edges[i + 1] - edges[i]) * ratio
                elif start < edges[i] and end >= edges[i] and end < edges[i + 1]:
                    cputime[i] += (end - edges[i]) * ratio
                    wall += (end - edges[i]) * ratio
                elif start >= edges[i] and start < edges[i + 1] and end >= edges[i + 1]:
                    cputime[i] += (edges[i + 1] - start) * ratio
                    wall += (edges[i + 1] - start) * ratio
            if abs(wall - cpu)/cpu > 0.1:
                logger.debug("time {0}: CPU {1}, {2} - {3}").format(wall, cpu, start, end)
        try:
            with open(cache, 'wb') as f:
                pickle.dump((cputime, ids), f)
        except IOError:
            logger.warning("could not save cpu time stats")

        cpu = np.zeros(len(reshape) - 1)

        if len(cputime) < len(cpu):
            logger.error("not enough data to produce cpu time plots.")
            return cpu

        for bin, low, high in zip(cputime, edges[:-1], edges[1:]):
            bins = np.digitize([low, high], reshape)
            if bins[0] == bins[1]:
                cpu[bins[0] - 1] += bin
            else:
                if bins[0] > 0:
                    cpu[bins[0] - 1] += bin * (reshape[bins[0]] - low) / 60.
                if bins[1] < len(cpu):
                    cpu[bins[1] - 1] += bin * (high - reshape[bins[1] - 1]) / 60.
        return cpu

    def plot(self, a, xlabel, stub=None, ylabel="Jobs", bins=100, modes=None, **kwargs_raw):
        kwargs = dict(kwargs_raw)
        if 'ymax' in kwargs:
            del kwargs['ymax']

        if not modes:
            modes = [Plotter.HIST, Plotter.PROF|Plotter.TIME]

        for mode in modes:
            filename = stub
            fig, ax = plt.subplots()

            # to pickle plot contents
            data = {'data': a, 'bins': bins, 'labels': kwargs.get('label')}

            if mode & Plotter.TIME:
                f = np.vectorize(self.unix2matplotlib)
                a = [(f(x), y) for (x, y) in a if len(x) > 0]

                data['data'] = a

                # interval = 2**math.floor(math.log((bins[-1] - bins[0]) / 9000.0) / math.log(2))
                # num_bins = map(self.unix2matplotlib, bins)
                # ax.xaxis.set_major_locator(dates.MinuteLocator(byminute=range(0, 60, 15), interval=24*60))
                ax.xaxis.set_major_formatter(dates.DateFormatter("%m-%d\n%H:%M"))
                ax.set_ylabel(xlabel)
            else:
                ax.set_xlabel(xlabel)
                ax.set_ylabel(ylabel)

            if mode & Plotter.HIST:
                filename += '-hist'

                if mode & Plotter.TIME:
                    ax.hist([x for (x, y) in a], weights=[y for (x, y) in a],
                            bins=bins, histtype='barstacked', **kwargs)
                else:
                    ax.hist([y for (x, y) in a], bins=bins, histtype='barstacked', **kwargs)
            elif mode & Plotter.PROF:
                filename += '-prof'
                data['data'] = []

                for i, (x, y) in enumerate(a):
                    borders = (self.unix2matplotlib(self.__xmin), self.unix2matplotlib(self.__xmax))
                    sums, edges = np.histogram(x, bins=bins, range=borders, weights=y)
                    squares, edges = np.histogram(x, bins=bins, range=borders, weights=np.multiply(y, y))
                    counts, edges = np.histogram(x, bins=bins, range=borders)
                    avg = np.divide(sums, counts)
                    avg_sq = np.divide(squares, counts)
                    err = np.sqrt(np.subtract(avg_sq, np.multiply(avg, avg)))

                    newargs = dict(kwargs)
                    if 'color' in newargs:
                        newargs['color'] = newargs['color'][i]
                    if 'label' in newargs:
                        newargs['label'] = newargs['label'][i]

                    centers = [.5 * (x + y) for x, y in zip(edges[:-1], edges[1:])]
                    ax.errorbar(centers, avg, yerr=err, fmt='o', ms=3, capsize=0, **newargs)

                    data['data'].append((centers, avg, err))

            elif mode & Plotter.PLOT:
                filename += '-plot'

                if 'label' in kwargs:
                    for (l, (x, y)) in zip(kwargs['label'], a):
                        ax.plot(x, y, label=l)
                else:
                    for (x, y) in a:
                        ax.plot(x, y)

            ax.grid(True)

            if mode & Plotter.TIME:
                ax.axis(
                        xmin=self.unix2matplotlib(self.__xmin),
                        xmax=self.unix2matplotlib(self.__xmax),
                        ymin=0
                )
            else:
                ax.axis(ymin=0)

            if 'ymax' in kwargs_raw:
                ax.axis(ymax=kwargs_raw['ymax'])

            if not mode & Plotter.TIME and mode & Plotter.HIST:
                labels = kwargs.get('label', [''] * len(a))
                stats = {}
                for label, (x, y) in zip(labels, a):
                    avg = np.average(y)
                    var = np.std(y)
                    med = np.median(y)
                    stats[label] = (avg, var, med)
                info = u"{0} μ = {1:.3g}, σ = {2:.3g} median = {3:.3g}"
                ax.text(0.75, 0.6,
                        '\n'.join([info.format(label + ':', avg, var, med) for label, (avg, var, med) in stats.items()]),
                        ha="center", transform=ax.transAxes, backgroundcolor='white')

            if 'label' in kwargs:
                ax.legend(bbox_to_anchor=(0.5, 0.9), loc='lower center', ncol=len(kwargs['label']), prop={'size': 7})

            self.pickle(filename, data)
            self.saveimg(filename)

    def make_pie(self, vals, labels, name, **kwargs):
        vals = [max(0, val) for val in vals]

        fig, ax = plt.subplots()
        fig.set_size_inches(4, 3)
        ratio = 0.75
        ax.set_position([0.3, 0.3, ratio * 0.7, 0.7])

        newlabels = []
        total = sum(vals)
        for label, val in zip(labels, vals):
            if float(val) / total < .025:
                newlabels.append('')
            else:
                newlabels.append(label)

        with open(os.path.join(self.__plotdir, name + '.dat'), 'w') as f:
            for l, v in zip(labels, vals):
                f.write('{0}\t{1}\n'.format(l, v))

        patches, texts = ax.pie([max(0, val) for val in vals], labels=newlabels, **kwargs)

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

        return self.saveimg(name)

    def pickle(self, name, data):
        logger.debug("Saving data for {0}".format(name))
        with open(os.path.join(self.__plotdir, name + '.pkl'), 'wb') as f:
            pickle.dump(data, f)

    def saveimg(self, name):
        logger.info("Saving {0}".format(name))

        plt.savefig(os.path.join(self.__plotdir, name + '.png'))
        # plt.savefig(os.path.join(self.__plotdir, name + '.pdf'))

        plt.close()

    def make_foreman_plots(self):
        tasks = []
        idleness = []
        efficiencies = []

        names = []

        for filename in self.__foremen:
            headers, stats = self.readlog(filename)

            foreman = os.path.basename(filename)

            if re.match('.*log+', foreman):
                foreman=foreman[:foreman.rfind('.')]
                foreman = string.strip(foreman)
            names.append(foreman)

            tasks.append((stats[:,headers['timestamp']], stats[:,headers['tasks_running']]))
            idleness.append((stats[:,headers['timestamp']], stats[:,headers['idle_percentage']]))
            efficiencies.append((stats[:,headers['timestamp']], stats[:,headers['efficiency']]))

            self.plot(
                    [
                        (stats[:,headers['timestamp']], stats[:,headers['workers_busy']]),
                        (stats[:,headers['timestamp']], stats[:,headers['workers_idle']]),
                        (stats[:,headers['timestamp']], stats[:,headers['total_workers_connected']])
                    ],
                    'Workers', foreman + '-workers',
                    modes=[Plotter.PLOT|Plotter.TIME],
                    label=['busy', 'idle', 'connected']
            )

            self.plot(
                [
                (stats[:,headers['timestamp']], stats[:,headers['total_workers_joined']]),
                (stats[:,headers['timestamp']], stats[:,headers['total_workers_removed']])
                ],
                'Workers', foreman + '-turnover',
                modes=[Plotter.HIST|Plotter.TIME],
                label=['joined', 'removed']
            )

            self.make_pie(
                [
                np.sum(stats[:,headers['total_good_execute_time']]),
                np.sum(stats[:,headers['total_execute_time']]) - np.sum(stats[:,headers['total_good_execute_time']])
                ],
                ["good execute time", "total-good execute time"],
                foreman + "-time-pie",
                colors=["green","red"]
            )

        self.plot(
            tasks,
            'Tasks', 'foreman-tasks',
            modes=[Plotter.PLOT|Plotter.TIME],
            label=names
        )

        self.plot(
            idleness,
            'Idle', 'foreman-idle',
            modes=[Plotter.PLOT|Plotter.TIME],
            label=names
        )

        self.plot(
            efficiencies,
            'Efficiency', 'foreman-efficiency',
            modes=[Plotter.PLOT|Plotter.TIME],
            label=names
        )

        return names

    def make_plots(self, xmin=None, xmax=None, foremen=None):
        self.__xmin = self.parsetime(xmin)
        self.__xmax = self.parsetime(xmax)

        self.__foremen = foremen if foremen else []

        headers, stats = self.readlog()
        good_jobs, failed_jobs, summary_data, completed_jobits, total_jobits, start_jobits, processed_lumis = self.readdb()

        success_jobs = good_jobs[good_jobs['type'] == 0]
        merge_jobs = good_jobs[good_jobs['type'] == 1]

        failed_processing = failed_jobs[failed_jobs['type'] == 0]
        failed_merging = failed_jobs[failed_jobs['type'] == 1]

        foremen_names = self.make_foreman_plots()

        self.plot(
                [
                    (stats[:,headers['timestamp']], stats[:,headers['workers_busy']]),
                    (stats[:,headers['timestamp']], stats[:,headers['workers_idle']]),
                    (stats[:,headers['timestamp']], stats[:,headers['total_workers_connected']])
                ],
                'Workers', 'workers',
                modes=[Plotter.PLOT|Plotter.TIME],
                label=['busy', 'idle', 'connected']
        )

        self.plot(
                [(stats[:,headers['timestamp']], stats[:,headers['tasks_running']])],
                'Tasks', 'tasks',
                modes=[Plotter.PLOT|Plotter.TIME],
                label=['running']
        )

        self.plot(
                [(stats[:,headers['timestamp']], np.divide(stats[:,headers['total_memory']], stats[:,headers['total_cores']]))],
                'Avg memory / core', 'memory-per-core',
                modes=[Plotter.PLOT|Plotter.TIME]
        )

        sent, edges = np.histogram(stats[:,headers['timestamp']], bins=100, weights=stats[:,headers['total_send_time']])
        received, _ = np.histogram(stats[:,headers['timestamp']], bins=edges, weights=stats[:,headers['total_receive_time']])
        created, _ = np.histogram(stats[:,headers['timestamp']], bins=edges, weights=stats[:,headers['total_create_time']])
        returned, _ = np.histogram(stats[:,headers['timestamp']], bins=edges, weights=stats[:,headers['total_return_time']])
        idle_total = np.multiply(
                stats[:,headers['timestamp']] - stats[0,headers['timestamp']],
                stats[:,headers['idle_percentage']]
        )
        idle_diff = (idle_total - np.roll(idle_total, 1, 0)) / 60.
        idle, _ = np.histogram(stats[:,headers['timestamp']], bins=edges, weights=idle_diff)
        other = np.maximum([(y - x) / 60. for x, y in zip(edges[:-1], edges[1:])] - sent - received - created - returned - idle, 0)
        all = other + sent + received + created + returned + idle
        centers = [.5 * (x + y) for x, y in zip(edges[:-1], edges[1:])]

        self.plot(
                [
                    (centers, np.divide(sent, all)),
                    (centers, np.divide(received, all)),
                    (centers, np.divide(created, all)),
                    (centers, np.divide(returned, all)),
                    (centers, np.divide(idle, all)),
                    (centers, np.divide(other, all))
                ],
                'Fraction', 'fraction',
                bins=100,
                modes=[Plotter.HIST|Plotter.TIME],
                label=['sending', 'receiving', 'creating', 'returning', 'idle', 'other'],
                ymax=1.
        )

        self.plot(
                [
                    (stats[:,headers['timestamp']], stats[:,headers['total_workers_joined']]),
                    (stats[:,headers['timestamp']], stats[:,headers['total_workers_removed']])
                ],
                'Workers', 'turnover',
                modes=[Plotter.HIST|Plotter.TIME],
                label=['joined', 'removed']
        )

        if len(good_jobs) > 0 or len(failed_jobs) > 0:
            self.make_pie(
                    [
                        np.sum(good_jobs['t_allput'] - good_jobs['t_goodput'])
                            + np.sum(failed_jobs['t_allput'] - failed_jobs['t_goodput']),
                        np.sum(failed_jobs['t_allput']),
                        np.sum(good_jobs['t_first_ev'] - good_jobs['t_send_start']),
                        np.sum(good_jobs['t_processing_end'] - good_jobs['t_first_ev']),
                        np.sum(good_jobs['t_recv_end'] - good_jobs['t_processing_end'])
                    ],
                    ["Eviction", "Failed", "Overhead", "Processing", "Stage-out"],
                    "time-pie",
                    colors=["crimson", "red", "dodgerblue", "green", "skyblue"]
            )

            datasets = []
            colors = []
            labels = []

            for jobs, label, success_color, merged_color, merging_color in [
                    (success_jobs, 'processing', 'green', 'lightgreen', 'darkgreen'),
                    (merge_jobs, 'merging', 'purple', 'fuchsia', 'darkorchid')]:
                code_map = {
                        2: (label + ' (status: successful)', success_color),
                        6: ('published', 'blue'),
                        7: (label + ' (status: merging)', merging_color),
                        8: (label + ' (status: merged)', merged_color)
                }
                codes, split_jobs = split_by_column(jobs, 'status')

                datasets += [(xs['t_retrieved'], [1] * len(xs['t_retrieved'])) for xs in split_jobs]
                colors += [code_map[code][1] for code in codes]
                labels += [code_map[code][0] for code in codes]

            if len(failed_jobs) > 0:
                datasets += [(xs['t_retrieved'], [1] * len(xs['t_retrieved'])) for xs in [failed_jobs]]
                colors += ['red']
                labels += ['failed']

            self.plot(
                    datasets,
                    'Jobs', 'all-jobs',
                    modes=[Plotter.HIST|Plotter.TIME],
                    label=labels,
                    color=colors
            )

        if len(good_jobs) > 0:
            completed, bins = np.histogram(completed_jobits['t_retrieved'], 100)
            total_completed = np.cumsum(completed)
            centers = [(x + y) / 2 for x, y in zip(bins[:-1], bins[1:])]

            self.plot(
                    [(centers, total_completed * (-1.) + start_jobits)],
                    'Jobits remaining', 'jobits-total',
                    bins=100,
                    modes=[Plotter.PLOT|Plotter.TIME]
            )

            output, bins = np.histogram(
                    success_jobs['t_retrieved'], 100,
                    weights=success_jobs['b_output'] / 1024.0**3
            )

            total_output = np.cumsum(output)
            centers = [(x + y) / 2 for x, y in zip(bins[:-1], bins[1:])]

            scale = 3600.0 / ((bins[1] - bins[0]) * 1024.0**3)

            self.plot(
                    [(success_jobs['t_retrieved'], success_jobs['b_output'] * scale)],
                    'Output (GB/h)', 'output',
                    bins=100,
                    modes=[Plotter.HIST|Plotter.TIME]
            )

            self.plot(
                    [(centers, total_output)],
                    'Output (GB)', 'output-total',
                    bins=100,
                    modes=[Plotter.PLOT|Plotter.TIME]
            )

            def integrate_wall((x, y)):
                indices = np.logical_and(stats[:,0] >= x, stats[:,0] < y)
                values = stats[indices,headers['tasks_running']]
                if len(values) > 0:
                    return np.sum(values) * (y - x) / len(values)
                return 0

            walltime = np.array(map(integrate_wall, zip(edges[:-1], edges[1:])))
            cputime = self.updatecpu(success_jobs, edges)

            centers = [(x + y) / 2 for x, y in zip(edges[:-1], edges[1:])]

            cputime[walltime == 0] = 0.
            walltime[walltime == 0] = 1e-6

            ratio = np.nan_to_num(np.divide(cputime * 1.0, walltime))

            self.plot(
                    [(centers, ratio)],
                    'CPU / Wall', 'cpu-wall',
                    bins=100,
                    modes=[Plotter.HIST|Plotter.TIME]
            )

            ratio = np.nan_to_num(np.divide(np.cumsum(cputime) * 1.0, np.cumsum(walltime)))

            self.plot(
                    [(centers, ratio)],
                    'Integrated CPU / Wall', 'cpu-wall-int',
                    bins=100,
                    modes=[Plotter.HIST|Plotter.TIME]
            )

            for prefix, jobs, failures in [('good-', success_jobs, failed_processing), ('merge-', merge_jobs, failed_merging)]:
                if len(jobs) == 0:
                    continue

                cache_map = {0: ('cold cache', 'lightskyblue'), 1: ('hot cache', 'navy'), 2: ('dedicated', 'darkorchid')}
                cache, split_jobs = split_by_column(jobs, 'cache')
                # plot timeline
                things_we_are_looking_at = [
                        # x-times              , y-times                                                                       , y-label                      , filestub             , color            , in pie
                        ([(x['t_wrapper_start'], x['t_allput'] - x['t_goodput']) for x in split_jobs]                          , 'Lost runtime'               , 'eviction'           , "crimson"        , False) , # red
                        ([(x['t_wrapper_start'], x['t_processing_end'] - x['t_wrapper_start']) for x in split_jobs]            , 'Runtime'                    , 'runtime'            , "green"          , False) , # red
                        ([(x['t_wrapper_start'], x['t_send_end'] - x['t_send_start']) for x in split_jobs]                     , 'Input transfer'             , 'transfer-in'        , "black"          , True)  , # gray
                        ([(x['t_wrapper_start'], x['t_wrapper_start'] - x['t_send_end']) for x in split_jobs]                  , 'Startup'                    , 'startup'            , "darkorchid"     , True)  , # blue
                        ([(x['t_wrapper_start'], x['t_wrapper_ready'] - x['t_wrapper_start']) for x in split_jobs]             , 'Release setup'              , 'setup-release'      , "navy"           , True)  , # blue
                        ([(x['t_wrapper_start'], x['t_stage_in'] - x['t_wrapper_ready']) for x in split_jobs]                  , 'Stage-in'                   , 'stage-in'           , "gray"           , True)  , # gray
                        ([(x['t_wrapper_start'], x['t_prologue'] - x['t_stage_in']) for x in split_jobs]                       , 'Prologue'                   , 'prologue'           , "orange"         , True)  , # yellow
                        ([(x['t_wrapper_start'], x['t_file_req'] - x['t_prologue']) for x in split_jobs]                       , 'CMSSW setup'                , 'setup-cms'          , "royalblue"      , True)  , # blue
                        ([(x['t_wrapper_start'], x['t_file_open'] - x['t_file_req']) for x in split_jobs]                      , 'File request'               , 'file-open'          , "fuchsia"        , True)  , # blue
                        ([(x['t_wrapper_start'], x['t_first_ev'] - x['t_file_open']) for x in split_jobs]                      , 'CMSSW job setup'            , 'setup-job'          , "dodgerblue"     , True)  , # blue
                        ([(x['t_wrapper_start'], x['t_wrapper_ready'] - x['t_wrapper_start']
                                               + x['t_first_ev'] - x['t_prologue']) for x in split_jobs]                       , 'Overhead'                   , 'overhead'           , "blue"           , False) , # blue
                        ([(x['t_wrapper_start'], x['t_cpu']) for x in split_jobs]                                              , 'Processing CPU'             , 'processing-cpu'     , "forestgreen"    , True)  , # green
                        ([(x['t_wrapper_start'], x['t_processing_end'] - x['t_first_ev'] - x['t_cpu']) for x in split_jobs]    , 'Non-CPU processing'         , 'processing-non-cpu' , "green"          , True)  , # green
                        ([(x['t_wrapper_start'], x['t_processing_end'] - x['t_first_ev']) for x in split_jobs]                 , 'Processing Total'           , 'processing'         , "mediumseagreen" , False) , # green
                        ([(x['t_wrapper_start'], x['t_epilogue'] - x['t_processing_end']) for x in split_jobs]                 , 'Epilogue'                   , 'epilogue'           , "khaki"          , True)  , # yellow
                        ([(x['t_wrapper_start'], x['t_stage_out'] - x['t_epilogue']) for x in split_jobs]                      , 'Stage-out'                  , 'stage-out'          , "silver"         , True)  , # gray
                        ([(x['t_wrapper_start'], x['t_recv_start'] - x['t_stage_out']) for x in split_jobs]                    , 'Output transfer wait'       , 'transfer-out-wait'  , "lightskyblue"   , True)  , # blue
                        ([(x['t_wrapper_start'], x['t_recv_end'] - x['t_recv_start']) for x in split_jobs]                     , 'Output transfer work_queue' , 'transfer-out-wq'    , "gainsboro"      , True)    # gray
                ]

                times_by_cache = [plot[0] for plot in things_we_are_looking_at if plot[-1]]
                self.make_pie(
                        [np.sum([np.sum(x[1]) for x in times]) for times in times_by_cache],
                        [plot[1] for plot in things_we_are_looking_at if plot[-1]],
                        prefix + "time-detail-pie",
                        colors=[plot[-2] for plot in things_we_are_looking_at if plot[-1]]
                )

                for a, label, filestub, color, pie in things_we_are_looking_at:
                    self.plot(
                        [(xtimes, ytimes / 60.) for xtimes, ytimes in a],
                        label+' (m)', prefix+filestub,
                        color=[cache_map[x][1] for x in cache],
                        label=[cache_map[x][0] for x in cache]
                    )

        if len(failed_jobs) > 0:
            logs = self.savelogs(failed_jobs)

            fail_labels, fail_values = split_by_column(failed_jobs, 'exit_code', threshold=0.025)

            self.make_pie(
                    [len(xs['t_retrieved']) for xs in fail_values],
                    fail_labels,
                    "failed-pie"
            )

            self.plot(
                    [(xs['t_retrieved'], [1] * len(xs['t_retrieved'])) for xs in fail_values],
                    'Failed jobs', 'failed-jobs',
                    modes=[Plotter.HIST|Plotter.TIME],
                    label=map(str, fail_labels)
            )
        else:
            logs = None

        jsons = self.savejsons(processed_lumis)

        env = jinja2.Environment(loader=jinja2.FileSystemLoader(
            os.path.join(os.path.dirname(__file__), 'data')))
        env.filters["datetime"] = lambda d: datetime.fromtimestamp(d).strftime('%a, %d %b %Y, %H:%M')
        env.tests["sum"] = lambda s: s == "Total"
        template = env.get_template('template.html')

        with open(os.path.join(self.__plotdir, 'index.html'), 'w') as f:
            f.write(template.render(
                id=self.__id,
                plot_time=time.time(),
                plot_starttime=self.__xmin,
                plot_endtime=self.__xmax,
                run_starttime=self.__total_xmin,
                run_endtime=self.__total_xmax,
                bad_jobs=len(failed_jobs) > 0,
                good_jobs=len(success_jobs) > 0,
                merge_jobs=len(merge_jobs) > 0,
                summary=summary_data,
                jsons=jsons,
                bad_logs=logs,
                foremen=foremen_names
            ).encode('utf-8'))

def plot(args):
    p = Plotter(args.config, args.outdir)
    p.make_plots(args.xmin, args.xmax, args.foreman_list)
