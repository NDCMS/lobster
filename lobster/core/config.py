import os
import pickle
import re

from lobster.util import Configurable

class Config(Configurable):
    """
    Top-level Lobster configuration object

    This configuration object will fully specify a Lobster project,
    including several :class:`~lobster.core.workflow.Workflow` instances
    and a :class:`~lobster.se.StorageConfiguration`.

    Parameters
    ----------
        label : str
            A string to identify this project by.  This will be used in the
            CMS dashboard, where 
        workdir : str
            The working directory to be used for the project.  Note that
            this should be on a local filesystem to avoid problems with the
            database.
        storage : StorageConfiguration
            The configuration for the storage element for output and input
            files.
        workflows : list
            A list of :class:`~lobster.core.workflow.Workflow` to process.
        advanced : AdvancedOptions
            More options for advanced users.
        plotdir : str
            A directory to store monitoring pages in.
        foremen_logs : list
            A list of :class:`str` pointing to the `WorkQueue` foremen logs.
    """

    _mutable = []

    def __init__(self, label, workdir, storage, workflows, advanced=None, plotdir=None,
            foremen_logs=None,
            base_directory=None, base_configuration=None, startup_directory=None):
        """
        Top-level configuration object for Lobster
        """
        self.label = label
        self.workdir = workdir
        self.plotdir = plotdir
        self.foremen_logs = foremen_logs
        self.storage = storage
        self.workflows = workflows
        self.advanced = advanced if advanced else AdvancedOptions()

        self.base_directory = base_directory
        self.base_configuration = base_configuration
        self.startup_directory = startup_directory

    @classmethod
    def load(cls, path):
        try:
            with open(os.path.join(path, 'config.pkl'), 'rb') as f:
                return pickle.load(f)
        except IOError:
            raise IOError("can't load configuration from {0}".format(os.path.join(path, 'config.pkl')))

    def save(self):
        with open(os.path.join(self.workdir, 'config.pkl'), 'wb') as f:
            pickle.dump(self, f)


class AdvancedOptions(Configurable):
    """
    Advanced options for tuning Lobster

    Parameters
    ----------
        use_dashboard : bool
            Use the CMS dashboard to report task status.
        abort_threshold : int
            After how many successful tasks outliers in runtime should be
            killed.
        abort_multiplier : int
            How many standard deviations a task is allowed to go over the
            average task runtime.
        bad_exit_codes : list
            A list of exit codes that are considered to come from bad
            workers.  As soon as a task returns with an exit code from this
            list, the worker it ran on will be blacklisted and no more
            tasks send to it.
        dump_core : bool
            Produce core dumps.  Useful to debug `WorkQueue`.
        full_monitoring : bool
            Produce full monitoring output.  Useful to debug `WorkQueue`.
        log_level : int
            How much logging output to show.  Goes from 1 to 5, where 1 is
            the most verbose (including a lot of debug output), and 5 is
            practically quiet.
        payload : int
            How many tasks to keep in the queue (minimum).  Note that the
            payload will increase with the number of cores available to
            Lobster.  This is just the minimum with no workers connected.
        renew_proxy : bool
            Have Lobster automatically renew CMS authentication
            credentials.
        threshold_for_failure : int
            How often a single unit may fail to be processed before Lobster
            will not attempt to process it any longer.
        threshold_for_skipping : int
            How often a single file may fail to be accessed before Lobster
            will not attempt to process it any longer.
        wq_max_retries : int
            How often `WorkQueue` will attempt to process a task before
            handing it back to Lobster.  `WorkQueue` will only reprocess
            evicted tasks automatically.
    """

    _mutable = ['threshold_for_failure', 'threshold_for_skipping']

    def __init__(self,
            use_dashboard=True,
            abort_threshold=10,
            abort_multiplier=4,
            bad_exit_codes=None,
            dump_core=False,
            full_monitoring=False,
            log_level=2,
            payload=10,
            renew_proxy=True,
            threshold_for_failure=30,
            threshold_for_skipping=30,
            wq_max_retries=10):
        self.use_dashboard = use_dashboard
        self.abort_threshold = abort_threshold
        self.abort_multiplier = abort_multiplier
        self.bad_exit_codes = bad_exit_codes if bad_exit_codes else [169]
        self.dump_core = dump_core
        self.full_monitoring = full_monitoring
        self.log_level = log_level
        self.payload = payload
        self.renew_proxy = renew_proxy
        self.threshold_for_failure = threshold_for_failure
        self.threshold_for_skipping = threshold_for_skipping
        self.wq_max_retries = wq_max_retries

    def __init__(self,
            use_dashboard=True,
            abort_threshold=10,
            abort_multiplier=4,
            bad_exit_codes=None,
            dump_core=False,
            full_monitoring=False,
            log_level=2,
            payload=10,
            renew_proxy=True,
            threshold_for_failure=30,
            threshold_for_skipping=30,
            wq_max_retries=10):
        self.use_dashboard = use_dashboard
        self.abort_threshold = abort_threshold
        self.abort_multiplier = abort_multiplier
        self.bad_exit_codes = bad_exit_codes if bad_exit_codes else [169]
        self.dump_core = dump_core
        self.full_monitoring = full_monitoring
        self.log_level = log_level
        self.payload = payload
        self.renew_proxy = renew_proxy
        self.threshold_for_failure = threshold_for_failure
        self.threshold_for_skipping = threshold_for_skipping
        self.wq_max_retries = wq_max_retries
