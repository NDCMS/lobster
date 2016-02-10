import datetime
import logging
import multiprocessing
import os
import re

from lobster.commands.plot import Plotter
from lobster.util import PartiallyMutable

logger = logging.getLogger('lobster.actions')

cmd_re = re.compile('^.* = [0-9]+$')

class DummyQueue(object):
    def start(*args):
        pass

    def put(*args):
        pass

    def get(*args):
        return None

class Actions(object):
    def __init__(self, config):
        fn = os.path.join(config.workdir, 'ipc')
        if not os.path.exists(fn):
            os.mkfifo(fn)
        self.fifo = os.fdopen(os.open(fn, os.O_RDONLY|os.O_NONBLOCK))
        self.__config = config

        if not config.plotdir:
            self.plotq = DummyQueue()
        else:
            logger.info('plots in {0} will be updated automatically'.format(config.plotdir))
            if config.foremen_logs:
                logger.info('foremen logs will be included from: {0}'.format(', '.join(config.foremen_logs)))
            plotter = Plotter(config)

            def plotf(q):
                while q.get() not in ('stop', None):
                    try:
                        plotter.make_plots(foremen=config.foremen_logs)
                    except Exception as e:
                        logger.error("plotting failed with: {}".format(e))
                        import traceback
                        traceback.print_exc()

            self.plotq = multiprocessing.Queue()
            self.plotp = multiprocessing.Process(target=plotf, args=(self.plotq,))
            self.plotp.start()
            logger.info('spawning process for automatic plotting with pid {0}'.format(self.plotp.pid))

        self.__last = datetime.datetime.now()

    def __del__(self):
        logger.info('shutting down process for automatic plotting with pid {0}'.format(self.plotp.pid))
        self.plotq.put('stop')
        self.plotp.join()

    def __communicate(self):
        cmds = map(str.strip, self.fifo.readlines())
        for cmd in cmds:
            logger.debug('received commands: {}'.format(cmd))
            if not cmd_re.match(cmd):
                logger.error('invalid command received: {}'.format(cmd))
                continue
            try:
                with PartiallyMutable.lockdown():
                    exec cmd in {'config': self.__config}, {}
                    self.__config.save()
            except Exception as e:
                logger.error('caught exeption from command: {}'.format(e))

    def take(self, force=False):
        self.__communicate()

        now = datetime.datetime.now()
        if (now - self.__last).seconds > 15 * 60 or force:
            self.plotq.put('plot')
            self.__last = now

