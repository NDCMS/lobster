import datetime
import imp
import logging
import multiprocessing
import os
import time
import traceback

from lobster.commands.plot import Plotter
from lobster import util

logger = logging.getLogger('lobster.actions')


def runplots(plotter, foremen):
    try:
        plotter.make_plots(foremen=foremen)
    except Exception as e:
        logger.error("plotting failed with: {}. Trace: {}".format(e, traceback.format_exc()))


class Actions(object):

    def __init__(self, config, source):
        self.config = config
        self.source = source

        if config.plotdir:
            logger.info('plots in {0} will be updated automatically'.format(config.plotdir))
            if config.foremen_logs:
                logger.info('foremen logs will be included from: {0}'.format(', '.join(config.foremen_logs)))
            self.plotter = Plotter(config)

        self.__last = datetime.datetime.now()
        self.__last_config_update = util.checkpoint(config.workdir, 'configuration_check')
        if not self.__last_config_update:
            self.__last_config_update = time.time()
            util.register_checkpoint(config.workdir, 'configuration_check', self.__last_config_update)

    def update_configuration(self):
        configfile = os.path.join(self.config.workdir, 'config.py')
        if self.__last_config_update < os.path.getmtime(configfile):
            try:
                logger.info('updating configuration')
                self.__last_config_update = time.time()
                new_config = imp.load_source('userconfig', configfile).config
                self.config.update(new_config)
                self.config.save()
                util.register_checkpoint(self.config.workdir, 'configuration_check', self.__last_config_update)
            except Exception:
                logger.exception('failed to update configuration:')
                util.PartiallyMutable.purge()

            for method, args in util.PartiallyMutable.changes():
                if method is None:
                    continue
                logger.debug("executing callback '{}' with arguments {}".format(method, args))
                attrs = method.split('.')
                call = self
                if attrs[0] not in ['config', 'source']:
                    logger.error('invalid registered callback: {}'.format(method))
                    continue
                try:
                    for attr in attrs:
                        call = getattr(call, attr)
                    call(*args)
                except Exception:
                    logger.exception("caught exception while executing callback '{}' with arguments {}".format(method, args))

    def take(self, force=False):
        self.update_configuration()

        if self.config.advanced.proxy and not self.config.advanced.proxy.check():
            logger.error("proxy expired!")
            from lobster.commands.process import Terminate
            Terminate().kill(self.config)

        now = datetime.datetime.now()
        if hasattr(self, 'plotter'):
            if (now - self.__last).seconds > 15 * 60 or force:
                if not force and hasattr(self, 'p') and self.p.is_alive():
                    logger.info('plotting still running, skipping')
                else:
                    if hasattr(self, 'p'):
                        self.p.join()
                    logger.info('starting plotting process')
                    self.p = multiprocessing.Process(target=runplots, args=(self.plotter, self.config.foremen_logs))
                    self.p.start()
                self.__last = now
