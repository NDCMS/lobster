# vim: foldmethod=marker
import os
import shutil
import tempfile

from lobster import cmssw, se
from lobster.cmssw.dataset import DatasetInfo
from lobster.core.task import TaskHandler
from lobster.core.unit import TaskUpdate, UnitStore
from lobster.core.config import Config, AdvancedOptions
from lobster.core.workflow import Workflow


class DummyInterface(object):

    def update_units(self, data):
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
            db.execute("delete from workflows")

    @classmethod
    def setup_class(cls):
        os.environ['LOCALRT'] = ''
        cls.workdir = tempfile.mkdtemp()
        cls.interface = UnitStore(
            Config(
                label='test',
                workdir=cls.workdir,
                storage=se.StorageConfiguration(output=['file://' + cls.workdir]),
                workflows=[],
                advanced=AdvancedOptions(proxy=False, osg_version="3.3")
            )
        )

    @classmethod
    def teardown_class(cls):
        pass

    def create_file_dataset(self, label, files, tasksize):
        info = DatasetInfo()
        info.file_based = True
        info.tasksize = tasksize

        for fn in ['/test/{0}.root'.format(i) for i in range(files)]:
            info.files[fn].lumis = [(-1, -1)]

        info.total_units = len(info.files.keys())
        info.path = ''

        return Workflow(label, None), info

    def create_dbs_dataset(self, label, lumi_events=100, lumis=14, filesize=3.5, tasksize=5):
        # {{{
        info = DatasetInfo()
        info.total_events = lumi_events * lumis
        info.tasksize = tasksize
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
                info.files[f].events = file_events
                info.files[f].lumis = file_lumis

                file_count += 1
                file_size = 0
                file_events = 0
                file_lumis = []

        if file_size > 0:
            f = '/test/{0}.root'.format(file_count)
            info.files[f].events = file_events
            info.files[f].lumis = file_lumis

        lumis = sum([finfo.lumis for finfo in info.files.values()], [])
        info.total_units = len(lumis)
        total_lumis = len(set(lumis))
        info.stop_on_file_boundary = (total_lumis != info.total_units)

        return Workflow(label, None), info
        # }}}

    def test_create_datasets(self):
        # {{{
        cfg, info = self.create_dbs_dataset('test', 100, 11, 2.2, 3)

        total = 0

        assert len(info.files) == 5
        for fn, finfo in info.files.items():
            total += finfo.events
            assert len(finfo.lumis) == 3
        assert total == 1100
        # }}}

    def test_handler(self):
        # {{{
        self.interface.register_dataset(
            *self.create_dbs_dataset(
                'test_handler', lumis=11, filesize=2.2, tasksize=3))
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_handler', 1)[0]

        handler = TaskHandler(123, 'test_handler', files, lumis, 'test', True)

        files_info = {
            u'/test/0.root': (220, [(1, 1), (1, 2), (1, 3)])
        }
        files_skipped = []
        events_written = 123

        update = TaskUpdate()
        file_update, unit_update = \
            handler.get_unit_info(False, update, files_info, files_skipped, events_written)

        assert update.units_processed == 3
        assert update.events_read == 220
        assert update.events_written == 123
        assert update.status == 2
        assert file_update == [(220, 0, 1)]
        assert unit_update == []
        # }}}

    def test_obtain(self):
        # {{{
        self.interface.register_dataset(
            *self.create_dbs_dataset(
                'test_obtain', lumis=20, filesize=2.2, tasksize=3))
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_obtain', 1)[0]

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                     (label,)).fetchone()

        assert jr == 3
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)

        (jr, jd) = self.interface.db.execute(
            "select units_running, units_done from files_test_obtain where filename='/test/0.root'").fetchone()

        assert jr == 3
        assert jd == 0
        # }}}

    def test_obtain_split(self):
        # {{{
        self.interface.register_dataset(
            *self.create_dbs_dataset('test_obtain_split', lumis=20, filesize=2.2, tasksize=10))
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_obtain_split', 1)[0]

        (stop_on_file_boundary,) = self.interface.db.execute(
            "select stop_on_file_boundary from workflows where label=?", ('test_obtain_split',)).fetchone()

        assert len(files) == 1
        assert stop_on_file_boundary == 1
        # }}}

    def test_return_good(self):
        # {{{
        self.interface.register_dataset(
            *self.create_dbs_dataset(
                'test_good', lumis=20, filesize=3.0, tasksize=6))
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_good', 1)[0]

        task_update = TaskUpdate(host='hostname', id=id)
        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            False,
            task_update,
            {
                '/test/0.root': (300, [(1, 1), (1, 2), (1, 3)]),
                '/test/1.root': (60, [(1, 4), (1, 5), (1, 6)])
            },
            [],
            100
        )

        self.interface.update_units({(label, "units_" + label): [(task_update, file_update, unit_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                     (label,)).fetchone()

        assert jr == 0
        assert jd == 6
        assert er == 360
        assert ew == 100

        (id, jr, jd, er) = self.interface.db.execute(
            "select id, units_running, units_done, events_read from files_test_good where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 3
        assert er == 300

        (id, jr, jd, er) = self.interface.db.execute(
            "select id, units_running, units_done, events_read from files_test_good where filename='/test/1.root'").fetchone()

        assert jr == 0
        assert jd == 3
        assert er == 60
        # }}}

    def test_return_good_split(self):
        # {{{
        self.interface.register_dataset(
            *self.create_dbs_dataset('test_good_split', lumis=20, filesize=2.2, tasksize=6))

        tasks = self.interface.pop_units('test_good_split', 1)
        assert(len(tasks) == 1)
        (id, label, files, lumis, arg, _) = tasks[0]

        task_update = TaskUpdate(host='hostname', id=id)
        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            False,
            task_update,
            {
                '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
            },
            [],
            100
        )

        self.interface.update_units({(label, "units_" + label): [(task_update, file_update, unit_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""", (label,)).fetchone()

        assert jr == 0
        assert jd == 3
        assert er == 220
        assert ew == 100

        (id, jr, jd, er) = self.interface.db.execute(
            "select id, units_running, units_done, events_read from files_test_good_split where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 3
        assert er == 220
        # }}}

    def test_return_bad(self):
        # {{{
        self.interface.register_dataset(*self.create_dbs_dataset('test_bad'))
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_bad', 1)[0]

        task_update = TaskUpdate(
            exit_code=123,
            host='hostname',
            id=id,
            submissions=1)

        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            True,
            task_update,
            {},
            [],
            0
        )

        self.interface.update_units(
            {(label, "units_" + label): [(task_update, file_update, unit_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                     (label,)).fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)

        (id, jr, jd, er) = self.interface.db.execute(
            "select id, units_running, units_done, events_read from files_test_bad where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        # }}}

    def test_return_bad_again(self):
        # {{{
        self.interface.register_dataset(*self.create_dbs_dataset(
            'test_bad_again', lumis=20, filesize=2.2, tasksize=6))
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_bad_again', 1)[0]

        task_update = TaskUpdate(
            exit_code=123,
            host='hostname',
            id=id,
            submissions=1)

        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            True,
            task_update,
            {
                '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                '/test/1.root': (220, [(1, 3), (1, 4), (1, 5)]),
                '/test/2.root': (160, [(1, 5), (1, 6)])
            },
            [],
            100
        )

        self.interface.update_units(
            {(label, "units_" + label): [(task_update, file_update, unit_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                     (label,)).fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)

        (id, jr, jd, er) = self.interface.db.execute(
            "select id, units_running, units_done, events_read from files_test_bad_again where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        # }}}

    def test_return_ugly(self):
        # {{{
        self.interface.register_dataset(
            *self.create_dbs_dataset(
                'test_ugly', lumis=11, filesize=3, tasksize=6))
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_ugly', 1)[0]

        task_update = TaskUpdate(host='hostname', id=id)
        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            False,
            task_update,
            {
                '/test/0.root': (120, [(1, 2), (1, 3)])
            },
            ['/test/1.root'],
            50
        )

        self.interface.update_units(
            {(label, "units_" + label): [(task_update, file_update, unit_update)]})

        skipped = list(
            self.interface.db.execute(
                "select skipped from files_{0}".format(label)))

        assert skipped == [(0,), (1,), (0,), (0,)]

        status = list(
            self.interface.db.execute(
                "select status from units_{0} where file=2 group by status".format(label)))

        assert status == [(3,)]

        (jr, jd, jl, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                units_left,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                         (label,)).fetchone()

        assert jr == 0
        assert jd == 2
        assert jl == 9
        assert er == 120
        assert ew == 50

        (id, jr, jd) = self.interface.db.execute(
            "select id, units_running, units_done from files_test_ugly where filename='/test/0.root'").fetchone()

        assert jr == 0
        assert jd == 2

        (id, jr, jd) = self.interface.db.execute(
            "select id, units_running, units_done from files_test_ugly where filename='/test/1.root'").fetchone()

        assert jr == 0
        assert jd == 0
        # }}}

    def test_return_uglier(self):
        # {{{
        self.interface.register_dataset(
            *self.create_dbs_dataset(
                'test_uglier', lumis=15, filesize=3, tasksize=8))
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_uglier', 1)[0]

        task_update = TaskUpdate(host='hostname', id=id, submissions=1)
        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            False,
            task_update,
            {
                '/test/0.root': (300, [(1, 1), (1, 2), (1, 3)]),
                '/test/1.root': (300, [(1, 4), (1, 5), (1, 6)]),
            },
            ['/test/2.root'],
            100
        )

        self.interface.update_units({(label, "units_" + label): [(task_update, file_update, unit_update)]})

        # grab another task
        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_uglier', 1)[0]

        task_update.id = id
        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            False,
            task_update,
            {
                '/test/2.root': (300, [(1, 7), (1, 8), (1, 9)]),
                '/test/3.root': (300, [(1, 10), (1, 11), (1, 12)]),
                '/test/4.root': (100, [(1, 13)]),
            },
            [],
            100
        )

        self.interface.update_units({(label, "units_" + label): [(task_update, file_update, unit_update)]})

        (jr, jd, jl, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                units_left,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                         (label,)).fetchone()

        assert jr == 0
        assert jd == 13
        assert jl == 2
        assert er == 1300
        assert ew == 200
        # }}}

    def test_file_obtain(self):
        # {{{
        self.interface.register_dataset(
            *self.create_file_dataset(
                'test_file_obtain', 5, 3))

        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_file_obtain', 1)[0]

        parameters = {'mask': {'lumis': None}}
        TaskHandler(id, label, files, lumis, None, True).adjust(
            parameters, [], [], se.StorageConfiguration({}))

        assert parameters['mask']['lumis'] is None

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                     (label,)).fetchone()

        assert jr == 3
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)
        # }}}

    def test_file_return_good(self):
        # {{{
        self.interface.register_dataset(
            *self.create_file_dataset(
                'test_file_return_good', 5, 3))

        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_file_return_good', 1)[0]

        task_update = TaskUpdate(host='hostname', id=id)
        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            False,
            task_update,
            {
                '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                '/test/1.root': (220, [(1, 3), (1, 4), (1, 5)]),
                '/test/2.root': (160, [(1, 5), (1, 6)])
            },
            [],
            100
        )

        self.interface.update_units(
            {(label, "units_" + label): [(task_update, file_update, unit_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                     (label,)).fetchone()

        assert jr == 0
        assert jd == 3
        assert er == 600
        assert ew == 100
        # }}}

    def test_file_return_bad(self):
        # {{{
        self.interface.register_dataset(
            *self.create_file_dataset(
                'test_file_return_bad', 5, 3))

        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_file_return_bad', 1)[0]

        task_update = TaskUpdate(exit_code=1234, host='hostname', id=id)
        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            True,
            task_update,
            {},
            [],
            0
        )

        self.interface.update_units(
            {(label, "units_" + label): [(task_update, file_update, unit_update)]})

        (jr, jd, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
                                                     (label,)).fetchone()

        assert jr == 0
        assert jd == 0
        assert er in (0, None)
        assert ew in (0, None)
        # }}}

    def test_file_return_ugly(self):
        # {{{
        self.interface.register_dataset(
            *self.create_file_dataset(
                'test_file_return_ugly', 5, 3))

        (id, label, files, lumis, arg, _) = self.interface.pop_units('test_file_return_ugly', 1)[0]

        task_update = TaskUpdate(host='hostname', id=id)
        handler = TaskHandler(id, label, files, lumis, None, True)
        file_update, unit_update = handler.get_unit_info(
            False,
            task_update,
            {
                '/test/0.root': (220, [(1, 1), (1, 2), (1, 3)]),
                '/test/1.root': (220, [(1, 3), (1, 4), (1, 5)]),
            },
            ['/test/2.root'],
            100
        )

        self.interface.update_units(
            {(label, "units_" + label): [(task_update, file_update, unit_update)]})

        (jr, jd, jl, er, ew) = self.interface.db.execute("""
            select
                units_running,
                units_done,
                units_left,
                (select sum(events_read) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id),
                (select sum(events_written) from tasks where status in (2, 6, 8) and type = 0 and workflow = workflows.id)
            from workflows where label=?""",
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
        cls.provider = cmssw.taskProvider({
            'workdir': cls.workdir,
            'stageout location': cls.workdir,
            'id': 'test',
            'recycle sandbox': '/dev/null',
            'tasks': {}
        })
        cls.provider._taskProvider__store = DummyInterface()

        shutil.copytree(
            os.path.join(os.path.dirname(__file__), 'tmp_data'),
            os.path.join(cls.workdir, 'tmp_data'))

    @classmethod
    def teardown_class(cls):
        shutil.rmtree(cls.workdir)

    # def test_return_good(self):
        # self.provider._taskProvider__taskdirs[0] = \
        # os.path.join(self.workdir, 'tmp_data', 'running', '0')
        # self.provider._taskProvider__taskworkflows[0] = 'test'
        # self.provider._taskProvider__taskoutputs[0] = []
        # self.provider

    # def test_return_bad(self):
        # assert 2 == 2

    # def test_return_ugly(self):
        # assert 1 == 1
