from lobster.core.command import Command


class Configuration(Command):
    @property
    def help(self):
        return 'dump the configuration in a human-readable fromat'

    def setup(self, argparser):
        pass

    def run(self, args):
        print args.config
