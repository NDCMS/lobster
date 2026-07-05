import os
import sys
import tempfile
import types
import unittest


def install_plot_dependency_stubs():
    cycler_module = types.ModuleType('cycler')
    cycler_module.cycler = lambda *args, **kwargs: None
    sys.modules.setdefault('cycler', cycler_module)

    jinja2_module = types.ModuleType('jinja2')
    jinja2_module.Environment = object
    jinja2_module.FileSystemLoader = object
    sys.modules.setdefault('jinja2', jinja2_module)

    matplotlib_module = types.ModuleType('matplotlib')
    matplotlib_module.use = lambda *args, **kwargs: None
    matplotlib_module.rc = lambda *args, **kwargs: None
    sys.modules.setdefault('matplotlib', matplotlib_module)
    sys.modules.setdefault('matplotlib.pyplot', types.ModuleType('matplotlib.pyplot'))
    sys.modules.setdefault('matplotlib.dates', types.ModuleType('matplotlib.dates'))
    sys.modules.setdefault('matplotlib.ticker', types.ModuleType('matplotlib.ticker'))

    scipy_module = types.ModuleType('scipy')
    scipy_interpolate = types.ModuleType('scipy.interpolate')
    scipy_interpolate.UnivariateSpline = object
    sys.modules.setdefault('scipy', scipy_module)
    sys.modules.setdefault('scipy.interpolate', scipy_interpolate)

    wmcore_module = types.ModuleType('WMCore')
    wmcore_datastructs = types.ModuleType('WMCore.DataStructs')
    wmcore_lumilist = types.ModuleType('WMCore.DataStructs.LumiList')
    wmcore_lumilist.LumiList = object
    sys.modules.setdefault('WMCore', wmcore_module)
    sys.modules.setdefault('WMCore.DataStructs', wmcore_datastructs)
    sys.modules.setdefault('WMCore.DataStructs.LumiList', wmcore_lumilist)

    lobster_core = types.ModuleType('lobster.core')
    lobster_core_unit = types.ModuleType('lobster.core.unit')
    lobster_core_command = types.ModuleType('lobster.core.command')
    lobster_core_command.Command = object
    sys.modules.setdefault('lobster.core', lobster_core)
    sys.modules.setdefault('lobster.core.unit', lobster_core_unit)
    sys.modules.setdefault('lobster.core.command', lobster_core_command)


install_plot_dependency_stubs()

try:
    from lobster.commands.plot import Plotter
except ImportError as exc:
    Plotter = None
    IMPORT_ERROR = exc
else:
    IMPORT_ERROR = None


HEADERS = [
    'timestamp',
    'units_left',
    'workers_joined',
    'workers_removed',
    'workers_lost',
    'workers_idled_out',
    'workers_fast_aborted',
    'workers_blacklisted',
    'workers_released',
]


@unittest.skipIf(Plotter is None, 'plot dependencies unavailable: {}'.format(IMPORT_ERROR))
class TestPlotterReadlog(unittest.TestCase):

    def make_plotter(self):
        plotter = Plotter.__new__(Plotter)
        plotter._Plotter__xmin = 0
        plotter._Plotter__xmax = 10
        return plotter

    def write_stats(self, lines):
        handle, path = tempfile.mkstemp(prefix='lobster_stats_', suffix='.log')
        with os.fdopen(handle, 'w') as statsfile:
            statsfile.write('#' + ' '.join(HEADERS) + '\n')
            for line in lines:
                statsfile.write(' '.join(map(str, line)) + '\n')
        self.addCleanup(lambda: os.path.exists(path) and os.unlink(path))
        return path

    def test_single_row_stats_are_read_as_two_dimensional_array(self):
        statsfile = self.write_stats([[1000000, 3, 1, 0, 0, 0, 0, 0, 0]])

        headers, stats = self.make_plotter().readlog(filename=statsfile)

        self.assertEqual(headers['timestamp'], 0)
        self.assertEqual(stats.shape, (1, len(HEADERS)))
        self.assertEqual(stats[0, headers['timestamp']], 1)

    def test_header_only_stats_return_empty_two_dimensional_array(self):
        statsfile = self.write_stats([])

        headers, stats = self.make_plotter().readlog(filename=statsfile)

        self.assertEqual(headers['timestamp'], 0)
        self.assertEqual(stats.shape, (0, len(HEADERS)))


if __name__ == '__main__':
    unittest.main()
