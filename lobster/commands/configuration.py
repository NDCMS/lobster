import os
import sys

from lobster.core.command import Command


class Configuration(Command):
    @property
    def help(self):
        return 'dump the configuration in python'

    def setup(self, argparser):
        pass

    def run(self, args):
        with open(os.path.join(args.config.workdir, 'config.py')) as fd:
            sys.stdout.write("".join(fd.readlines()))
