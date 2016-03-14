from abc import ABCMeta, abstractmethod, abstractproperty
import glob
import imp
import os

class CommandRegistry(ABCMeta):
    def __init__(cls, name, bases, attrs):
        if not hasattr(cls, 'plugins'):
            cls.plugins = {}
        elif name.lower() not in cls.plugins:
            cls.plugins[name.lower()] = cls()
        super(CommandRegistry, cls).__init__(name, bases, attrs)

    def register(cls, dirnames, parser):
        for dirname in dirnames:
            for fn in glob.glob(os.path.join(dirname, '*.py')):
                name = os.path.basename(fn)[:-3]
                source = imp.load_source(name, fn)
        subparsers = parser.add_subparsers(title='commands')
        for name, plugin in cls.plugins.items():
            parser = subparsers.add_parser(name, help=plugin.help)
            plugin.setup(parser)
            parser.set_defaults(plugin=plugin)

class Command(object):
    __metaclass__ = CommandRegistry

    @abstractproperty
    def help(self):
        pass

    @property
    def daemonizable(self):
        return False

    @abstractmethod
    def setup(self, argparser):
        pass

    @abstractmethod
    def run(self, args):
        pass
