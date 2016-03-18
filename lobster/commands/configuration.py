from lobster.core.command import Command


class Configuration(Command):
    @property
    def help(self):
        return 'dump the configuration in python'

    def setup(self, argparser):
        pass

    def run(self, args):
        print args.config
