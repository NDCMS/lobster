import logging
import os
import subprocess

from lobster import util
from lobster.core.command import Command

class Configure(Command):
    @property
    def help(self):
        return 'change the configuration of a running lobster process'

    def setup(self, argparser):
        pass

    def run(self, args):
        logger = logging.getLogger('lobster.configure')

        try:
            subprocess.check_call([
                os.environ.get('EDITOR', 'vi'),
                os.path.join(args.config.workdir, 'config.py')])
            logger.info("check {} to see if changes got propagated (may take a few minutes)".format(
                os.path.join(args.config.workdir, 'configure.log')))
        except Exception as e:
            logger.error("error editing current configuration: {}".format(e))
