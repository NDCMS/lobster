# vim: foldmethod=marker
from lobster import cmssw
from lobster.cmssw.dataset import DatasetInfo
from lobster.cmssw.job import JobHandler
import os
import shutil
import tempfile

from FWCore.PythonUtilities.LumiList import LumiList

class DummyInterface(object):
    def update_jobits(self, data):
        self.data = data

class DummyTask(object):
    def __init__(self, id, code):
        self.tag = id
        self.return_status = code
        self.output = None
        # TODO finish

class TestSQLBackend(object):
    def setup(self):
        with self.interface.db as db:
            db.execute("delete from datasets")

    @classmethod
    def setup_class(cls):
        cls.workdir = tempfile.mkdtemp()
        cls.interface = cmssw.JobitStore({
            'workdir': cls.workdir,
            'stageout location': cls.workdir,
            'id': 'test',
            'recycle sandbox': '/dev/null',
            'tasks': {}
        })

    @classmethod
    def teardown_class(cls):
        pass

    def create_file_dataset(self, label, files, jobsize):
        info = DatasetInfo()
        info.file_based = True
        info.jobsize = jobsize

        info.files = ['/test/{0}.root'.format(i) for i in range(files)]
        info.lumis = dict((file, [(-1, -1)]) for file in info.files)

        info.total_lumis = len(info.files)
        info.path = ''

        cfg = {
                'dataset': '/Test',
                'global tag': 'test',
                'label': label,
                'cmssw config': ''
        }

        return cfg, info, lambda x: x

    def create_dbs_dataset(self, label, lumi_events=100, lumis=14, filesize=3.5, jobsize=5):
        # {{{
        info = DatasetInfo()
        info.total_events = lumi_events * lumis
        info.total_lumis = lumis
        info.jobsize = jobsize
        info.path = ''

        file_size = 0
        file_count = 0
        file_events = 0
        file_lumis = []

        events = map(list, enumerate([lumi_events] * lumis))
        while len(events) > 0:
            (lumi, size) = events[0]

            file_size += float(size) / lumi_events
            file_events += size
            file_lumis.append((1, lumi + 1))

            if file_size > filesize:
                remove = file_size - filesize
                remove_events = int(lumi_events * remove)
                file_size -= remove
                file_events -= remove_events
                events[0][1] = remove_events
            else:
                events.pop(0)

            if file_size == filesize:
                f = '/test/{0}.root'.format(file_count)
                info.event_counts[f] = file_events
                info.lumis[f] = file_lumis
                info.files.append(f)

                file_count += 1
                file_size = 0
                file_events = 0
                file_lumis = []

        if file_size > 0:
            f = '/test/{0}.root'.format(file_count)
            info.event_counts[f] = file_events
            info.lumis[f] = file_lumis
            info.files.append(f)

        cfg = {
                'dataset': '/Test',
                'global tag': 'test',
                'label': label,
                'cmssw config': ''
        }

        return cfg, info, lambda x: x
        # }}}

    def test_create_datasets(self):
        # {{{
        cfg, info, _ = self.create_dbs_dataset('test', 100, 11, 2.2, 3)

        total = 0

        assert len(info.files) == 5
        for (k, v) in info.lumis.items():
            total += info.event_counts[k]
            assert len(v) == 3

        assert total == 1100
        # }}}

    def test_handler(self):
        # {{{
        self.interface.register(
                *self.create_dbs_dataset(
                    'test_handler', lumis=11, filesize=2.2, jobsize=3))
        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        handler = JobHandler(123, 'test_handler', files, lumis, 'test', True)

        files_info = {
                u'/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                u'/test/1.root': (80, [(1, 3)])
        }
        files_skipped = []
        events_written = 123

        job_update, file_update, lumi_update = handler.get_jobit_info(False, files_info, files_skipped, events_written)

        assert job_update == [0, 300, 123, 2]
        assert file_update == [(220, 0, 1), (80, 0, 2)]
        assert lumi_update == []
        # }}}

    def test_obtain(self):
        # {{{
        self.interface.register(
                *self.create_dbs_dataset(
                    'test_obtain', lumis=20, filesize=2.2, jobsize=3))
        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        (jr, jd, er, ew) = self.interface.db.execute(
                "select jobits_running, jobits_done, (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id), (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id) from datasets where label=?",
                (label,)
                ).fetchone()

        print jr
        assert jr == 4
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)

        (jr, jd) = self.interface.db.execute(
                "select jobits_running, jobits_done from files_test_obtain where filename='/test/0.root'").fetchone()

        assert jr == 3
        assert jd == 0
        # }}}

    def test_return_good(self):
        # {{{
        self.interface.register(
                *self.create_dbs_dataset(
                    'test_good', lumis=20, filesize=2.2, jobsize=6))
        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        data = [0] * 7
        exit_code = 0
        submissions = 0
        times = [0] * 19

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        False,
                        {
                            '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                            '/test/1.root': (220, [(1, 3), (1, 4), (1, 5)]),
                            '/test/2.root': (60, [(1, 5)])
                        },
                        [],
                        100
                        )
        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 0
        assert jd == 7
        assert er == 500
        assert ew == 100

        (id, jr, jd, er) = self.interface.db.execute(
                "select id, jobits_running, jobits_done, events_read from files_test_good where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 3
        assert er == 220

        (id, jr, jd, er) = self.interface.db.execute(
                "select id, jobits_running, jobits_done, events_read from files_test_good where filename='/test/2.root'").fetchone()

        assert jr == 0
        assert jd == 1
        assert er == 60
        # }}}

    def test_return_bad(self):
        # {{{
        self.interface.register(*self.create_dbs_dataset('test_bad'))
        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        data = [0] * 7
        exit_code = 123
        submissions = 1
        times = [0] * 19

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        True,
                        {},
                        [],
                        0
                        )
        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)

        (id, jr, jd, er) = self.interface.db.execute(
                "select id, jobits_running, jobits_done, events_read from files_test_bad where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        # }}}

    def test_return_bad_again(self):
        # {{{
        self.interface.register(*self.create_dbs_dataset(
            'test_bad_again', lumis=20, filesize=2.2, jobsize=6))
        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        data = [0] * 7
        exit_code = 123
        submissions = 1
        times = [0] * 19

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        True,
                        {
                            '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                            '/test/1.root': (220, [(1, 3), (1, 4), (1, 5)]),
                            '/test/2.root': (160, [(1, 5), (1, 6)])
                        },
                        [],
                        100
                        )

        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)

        (id, jr, jd, er) = self.interface.db.execute(
                "select id, jobits_running, jobits_done, events_read from files_test_bad_again where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        # }}}

    def test_return_ugly(self):
        # {{{
        self.interface.register(
                *self.create_dbs_dataset(
                    'test_ugly', lumis=11, filesize=2.2, jobsize=6))
        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        data = [0] * 7
        exit_code = 0
        submissions = 1
        times = [0] * 19

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        False,
                        {
                            '/test/0.root': (120, [(1, 2), (1, 3)])
                        },
                        ['/test/1.root', '/test/2.root'],
                        50
                        )
        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        skipped = list(
                self.interface.db.execute(
                    "select skipped from files_{0}".format(label)))

        assert skipped == [(0,), (1,), (1,), (0,), (0,)]

        status = list(
                self.interface.db.execute(
                    "select status from jobits_{0} where file=2 group by status".format(label)))

        assert status == [(3,)]

        (jr, jd, jl, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                jobits_left,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 0
        assert jd == 2
        assert jl == 13
        assert er == 120
        assert ew == 50

        (id, jr, jd) = self.interface.db.execute(
                "select id, jobits_running, jobits_done from files_test_ugly where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 2

        (id, jr, jd) = self.interface.db.execute(
                "select id, jobits_running, jobits_done from files_test_ugly where filename='/test/1.root'").fetchone()

        assert jr == 0
        assert jd == 0
        # }}}

    def test_return_uglier(self):
        # {{{
        self.interface.register(
                *self.create_dbs_dataset(
                    'test_uglier', lumis=11, filesize=2.2, jobsize=6))
        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        data = [0] * 7
        exit_code = 0
        submissions = 1
        times = [0] * 19

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        False,
                        {
                            '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                            '/test/1.root': (220, [(1, 3), (1, 4), (1, 5)]),
                        },
                        ['/test/2.root'],
                        100
                        )
        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        # grab another job
        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        False,
                        {
                            '/test/2.root': (220, [(1, 5), (1, 6), (1, 7)]),
                            '/test/3.root': (220, [(1, 7), (1, 8), (1, 9)]),
                            '/test/4.root': (20, [(1, 9)]),
                        },
                        [],
                        100
                        )
        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        (jr, jd, jl, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                jobits_left,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 0
        assert jd == 13
        assert jl == 2
        assert er == 900
        assert ew == 200
        # }}}

    def test_file_obtain(self):
        # {{{
        self.interface.register(
                *self.create_file_dataset(
                    'test_file_obtain', 5, 3))

        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        job_files, job_lumis = JobHandler(id, label, files, lumis, None, True).get_job_info()

        assert job_lumis == None

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 3
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)
        # }}}

    def test_file_return_good(self):
        # {{{
        self.interface.register(
                *self.create_file_dataset(
                    'test_file_return_good', 5, 3))

        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        data = [0] * 7
        exit_code = 0
        submissions = 0
        times = [0] * 19

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        False,
                        {
                            '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                            '/test/1.root': (220, [(1, 3), (1, 4), (1, 5)]),
                            '/test/2.root': (160, [(1, 5), (1, 6)])
                        },
                        [],
                        100
                        )
        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 0
        assert jd == 3
        assert er == 600
        assert ew == 100
        # }}}

    def test_file_return_bad(self):
        # {{{
        self.interface.register(
                *self.create_file_dataset(
                    'test_file_return_bad', 5, 3))

        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        data = [0] * 7
        exit_code = 1234
        submissions = 0
        times = [0] * 19

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        True,
                        {},
                        [],
                        0
                        )
        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)
        # }}}

    def test_file_return_ugly(self):
        # {{{
        self.interface.register(
                *self.create_file_dataset(
                    'test_file_return_ugly', 5, 3))

        (id, label, files, lumis, arg, _, _) = self.interface.pop_jobits()[0]

        data = [0] * 7
        exit_code = 0
        submissions = 0
        times = [0] * 19

        handler = JobHandler(id, label, files, lumis, None, True)
        job_update, file_update, lumi_update = \
                handler.get_jobit_info(
                        False,
                        {
                            '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                            '/test/1.root': (220, [(1, 3), (1, 4), (1, 5)]),
                        },
                        ['/test/2.root'],
                        100
                        )
        job_update = ['hostname', exit_code, submissions] + times + data + job_update + [id]

        self.interface.update_jobits({(label, "jobits_" + label): [(job_update, file_update, lumi_update)]})

        (jr, jd, jl, er, ew) = self.interface.db.execute("""
            select
                jobits_running,
                jobits_done,
                jobits_left,
                (select sum(events_read) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id),
                (select sum(events_written) from jobs where status in (2, 6, 8) and type = 0 and dataset = datasets.id)
            from datasets where label=?""",
            (label,)).fetchone()

        assert jr == 0
        assert jd == 2
        assert jl == 3
        assert er == 440
        assert ew == 100
        # }}}

class TestCMSSWProvider(object):
    @classmethod
    def setup_class(cls):
        cls.workdir = tempfile.mkdtemp()
        cls.provider = cmssw.JobProvider({
            'workdir': cls.workdir,
            'stageout location': cls.workdir,
            'id': 'test',
            'recycle sandbox': '/dev/null',
            'tasks': {}
        })
        cls.provider._JobProvider__store = DummyInterface()

        shutil.copytree(
                os.path.join(os.path.dirname(__file__), 'data'),
                os.path.join(cls.workdir, 'data'))

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.workdir)

    # def test_return_good(self):
        # self.provider._JobProvider__jobdirs[0] = \
                # os.path.join(self.workdir, 'data', 'running', '0')
        # self.provider._JobProvider__jobdatasets[0] = 'test'
        # self.provider._JobProvider__joboutputs[0] = []
        # self.provider

    # def test_return_bad(self):
        # assert 2 == 2

    # def test_return_ugly(self):
        # assert 1 == 1
