from lobster import cmssw
from lobster.cmssw.dataset import DatasetInfo
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

    def create_dbs_dataset(self, label, lumi_events=100, lumis=14, filesize=3.5, jobsize=5):
        info = DatasetInfo()
        info.total_events = lumi_events * lumis
        info.total_lumis = lumis
        info.jobsize = jobsize

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

        return cfg, info

    def test_create_datasets(self):
        cfg, info = self.create_dbs_dataset('test', 100, 11, 2.2, 3)

        total = 0

        assert len(info.files) == 5
        for (k, v) in info.lumis.items():
            total += info.event_counts[k]
            assert len(v) == 3

        assert total == 1100

    def test_return_obtain(self):
        self.interface.register(
                *self.create_dbs_dataset(
                    'test_obtain', lumis=20, filesize=2.2, jobsize=15))
        (id, label, files, lumis) = self.interface.pop_jobits()[0]
        (jr, jd, er, ew) = self.interface.db.execute(
                "select jobits_running, jobits_done, events_read, events_written from datasets where label='test_obtain'"
                ).fetchone()
        assert jr == 15
        assert jd == 0
        assert er == 0
        assert ew == 0

    def test_return_good(self):
        self.interface.register(
                *self.create_dbs_dataset(
                    'test_good', lumis=20, filesize=2.2, jobsize=15))
        (id, label, files, lumis) = self.interface.pop_jobits()[0]
        times = [0] * 14
        data = [0, 0]
        self.interface.update_jobits({'test_good': [(
            id, "", False, 0, 1,
            lumis.getLumis(), [], [],
            times, data, {'/test/0.root': 100, '/test/1.root': 50}, 100,
        )]})
        (jr, jd, er, ew) = self.interface.db.execute(
                "select jobits_running, jobits_done, events_read, events_written from datasets where label='test_good'"
                ).fetchone()
        assert jr == 0
        assert jd == 15
        assert er == 150
        assert ew == 100

    def test_return_bad(self):
        self.interface.register(*self.create_dbs_dataset('test_bad'))
        (id, label, files, lumis) = self.interface.pop_jobits()[0]
        times = [0] * 14
        data = [0, 0]
        self.interface.update_jobits({'test_bad': [(
            id, "", True, 8008, 1,
            [], lumis.getLumis(), files,
            times, data, {}, 0
        )]})
        (jr, jd, er, ew) = self.interface.db.execute(
                "select jobits_running, jobits_done, events_read, events_written from datasets where label='test_bad'"
                ).fetchone()
        assert jr == 0
        assert jd == 0
        assert er == 0
        assert ew == 0

    def test_return_ugly(self):
        self.interface.register(
                *self.create_dbs_dataset(
                    'test_ugly', lumis=20, filesize=2.2, jobsize=15))
        (id, label, files, lumis) = self.interface.pop_jobits()[0]
        times = [0] * 14
        data = [0, 0]
        ls_processed = lumis.getLumis()[:11]
        ls_skipped = lumis.getLumis()[11:]
        self.interface.update_jobits({'test_ugly': [(
            id, "", True, 8008, 1,
            ls_processed, ls_skipped, [],
            times, data, {files[0]: 1000, files[1]: 100}, 0
        )]})
        (jr, jd, er, ew) = self.interface.db.execute(
                "select jobits_running, jobits_done, events_read, events_written from datasets where label='test_ugly'"
                ).fetchone()
        assert jr == 0
        assert jd == 11
        assert er == 1100
        assert ew == 0

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
