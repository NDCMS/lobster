import datetime
import logging
import multiprocessing
import os
import re
import time

from lobster.commands.plot import Plotter
from lobster.util import PartiallyMutable

logger = logging.getLogger('lobster.actions')

cmd_re = re.compile('^.* = [0-9]+$')

def runplots(plotter, foremen):
    try:
        plotter.make_plots(foremen=foremen)
    except Exception as e:
        logger.error("plotting failed with: {}".format(e))
        import traceback
        traceback.print_exc()

class DummyQueue(object):
    def start(*args):
        pass

    def put(*args):
        pass

    def get(*args):
        return None

class Actions(object):
    def __init__(self, config, source):
        fn = os.path.join(config.workdir, 'ipc')
        if not os.path.exists(fn):
            os.mkfifo(fn)
        self.fifo = os.fdopen(os.open(fn, os.O_RDONLY|os.O_NONBLOCK))
        self.config = config
        self.source = source

        if config.plotdir:
            logger.info('plots in {0} will be updated automatically'.format(config.plotdir))
            if config.foremen_logs:
                logger.info('foremen logs will be included from: {0}'.format(', '.join(config.foremen_logs)))
            self.plotter = Plotter(config)

        self.__last = datetime.datetime.now()

    def __communicate(self):
        cmds = map(str.strip, self.fifo.readlines())
        for cmd in cmds:
            logger.debug('received commands: {}'.format(cmd))
            if not cmd_re.match(cmd):
                logger.error('invalid command received: {}'.format(cmd))
                continue
            try:
                with PartiallyMutable.lockdown():
                    exec cmd in {'config': self.config}, {}
                    self.config.save()
            except Exception as e:
                logger.error('caught exeption from command: {}'.format(e))
        for component, attr, args in PartiallyMutable.changes():
            if attr is not None:
                logger.info('updating setup by calling {} of {} with {}'.format(attr, component, args))
                getattr(getattr(self, component), attr)(*args)

    def take(self, force=False):
        self.__communicate()

        now = datetime.datetime.now()
        if hasattr(self, 'plotter'):
            if (now - self.__last).seconds > 15 * 60 or force:
                if not force and hasattr(self, 'p') and self.p.is_alive():
                    logger.info('plotting still running, skipping')
                else:
                    if force and hasattr(self, 'p'):
                        self.p.join()
                    logger.info('starting plotting process')
                    self.p = multiprocessing.Process(target=runplots, args=(self.plotter, self.config.foremen_logs))
                    self.p.start()
                self.__last = now

