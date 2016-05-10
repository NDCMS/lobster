import logging
import os

from lobster import util
from lobster.core.command import Command
from lockfile import AlreadyLocked

class Configure(Command):
    @property
    def help(self):
        return 'change the configuration of a running lobster process'

    def setup(self, argparser):
        argparser.add_argument('command', help='a python expression to change a mutable configuration setting')

    def run(self, args):
        logger = logging.getLogger('lobster.configure')

        try:
            pidfile = util.get_lock(args.config.workdir)
            logger.info("Lobster process not running, directly changing configuration.")
            with util.PartiallyMutable.lockdown():
                exec args.command in {'config': args.config, 'storage': args.config.storage}, {}
                args.config.save()
        except AlreadyLocked:
            logger.info("Lobster process still running, contacting process...")
            logger.info("sending command: " + args.command)
            logger.info("check the log of the main process for success")

            icp = open(os.path.join(args.config.workdir, 'ipc'), 'w')
            icp.write(args.command + '\n')
        except Exception as e:
            logger.error("can't change values: {}".format(e))
