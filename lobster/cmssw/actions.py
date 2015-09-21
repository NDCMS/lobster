import datetime
import multiprocessing

from lobster.cmssw.plotting import Plotter

logger = multiprocessing.get_logger()

class DummyQueue(object):
    def start(*args):
        pass

    def put(*args):
        pass

    def get(*args):
        return None

class Actions(object):
    def __init__(self, config):
        if 'plotdir' not in config:
            self.plotq = DummyQueue()
        else:
            logger.info('plots in {0} will be updated automatically'.format(config['plotdir']))
            if 'foremen logs' in config:
                logger.info('foremen logs will be included from: {0}'.format(', '.join(config['foremen logs'])))
            plotter = Plotter(config['workdir'], config['plotdir'])

            def plotf(q):
                while q.get() not in ('stop', None):
                    plotter.make_plots(foremen=config.get('foremen logs'))

            self.plotq = multiprocessing.Queue()
            self.plotp = multiprocessing.Process(target=plotf, args=(self.plotq,))
            self.plotp.start()
            logger.info('spawning process for automatic plotting with pid {0}'.format(self.plotp.pid))

        self.__last = datetime.datetime.now()

    def __del__(self):
        self.plotq.put('stop')

    def take(self):
        now = datetime.datetime.now()
        if (now - self.__last).seconds > 15 * 60:
            self.plotq.put('plot')
            self.__last = now

