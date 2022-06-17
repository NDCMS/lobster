import os
import sys

from argparse import ArgumentParser
import logging

from lobster.core import command, config
from lobster import util

# Restore some old code
# FIXME pycurl shipping with CMSSW is too old to harmonize with modern DBS!
rm = []
for f in sys.path:
    if '/cvmfs' in f:
        for pkg in ('numpy', 'matplotlib'):
            if pkg in f:
                rm.append(f)
for f in rm:
    sys.path.remove(f)

logger = logging.getLogger('lobster')


def boil():
    parser = ArgumentParser(description='A task submission tool for CMS')
    parser.add_argument('--verbose', '-v', action='count', default=0, help='increase verbosity')
    parser.add_argument('--quiet', '-q', action='count', default=0, help='decrease verbosity')

    command.Command.register([os.path.join(os.path.dirname(__file__), d, 'commands') for d in ['.', 'cmssw']], parser)

    parser.add_argument(metavar='{configfile,workdir}', dest='checkpoint',
                        help='configuration file to use or working directory to resume.')

    args = parser.parse_args()

    if os.path.isfile(args.checkpoint):
        try:
            import imp
            cfg = imp.load_source('userconfig', args.checkpoint).config
        except Exception as e:
            parser.error("the configuration '{0}' is not valid: {1}".format(args.checkpoint, e))

        if util.checkpoint(cfg.workdir, 'version'):
            cfg = config.Config.load(cfg.workdir)
        elif args.plugin.__class__.__name__.lower() == 'process':
            # This is the original configuration file!
            with util.PartiallyMutable.unlock():
                cfg.base_directory = os.path.abspath(os.path.dirname(args.checkpoint))
                cfg.base_configuration = os.path.abspath(args.checkpoint)
                cfg.startup_directory = os.path.abspath(os.getcwd())
                for w in cfg.workflows:
                    try:
                        w.validate()
                    except Exception as e:
                        parser.error("configuration '{0}' failed validation: {1}".format(args.checkpoint, e))
        else:
            parser.error("""
                Cannot find working directory at '{0}'.
                Have you run 'lobster process {1}'?
                If so, check if you have specified the working directory to change
                programatically (for example, with a timestamp appended). In that
                case, you will need to pass the desired working directory instead of
                configuration file.
                """.format(cfg.workdir, args.checkpoint))
    elif os.path.isdir(args.checkpoint):
        # Load configuration from working directory passed to us
        workdir = args.checkpoint
        try:
            cfg = config.Config.load(workdir)
        except Exception as e:
            parser.error("the working directory '{0}' does not contain a valid configuration: {1}".format(workdir, e))
        with util.PartiallyMutable.unlock():
            cfg.workdir = workdir
    else:
        parser.error("the working directory or configuration '{0}' does not exist".format(args.checkpoint))

    args.config = cfg
    args.preserve = []

    # Handle logging for everything in only one place!
    level = max(1, args.config.advanced.log_level + args.quiet - args.verbose) * 10
    logger.setLevel(level)

    formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    if args.plugin.daemonizable:
        fn = args.plugin.__class__.__name__.lower() + '.log'
        logger.info("saving log to {0}".format(os.path.join(cfg.workdir, fn)))
        if not os.path.isdir(cfg.workdir):
            os.makedirs(cfg.workdir)
        fileh = logging.handlers.RotatingFileHandler(os.path.join(cfg.workdir, fn), maxBytes=100e6, backupCount=10)
        fileh.setFormatter(formatter)
        fileh.setLevel(logging.INFO)
        for p in args.plugin.blacklisted_logs():
            fileh.addFilter(util.InvertedFilter('lobster.' + p))
        args.preserve.append(fileh.stream)
        logger.addHandler(fileh)

        if level < logging.INFO:
            fn = args.plugin.__class__.__name__.lower() + '_debug.log'
            logger.info("saving debug log to {0}".format(os.path.join(cfg.workdir, fn)))
            debugh = logging.handlers.RotatingFileHandler(os.path.join(cfg.workdir, fn), maxBytes=100e6, backupCount=10)
            debugh.setFormatter(formatter)
            args.preserve.append(debugh.stream)
            logger.addHandler(debugh)

        if not getattr(args, "foreground", False):
            logger.removeHandler(console)

    for p in args.plugin.additional_logs():
        fn = p + '.log'
        lg = logging.getLogger('lobster.' + p)
        logger.info("saving additional log for {1} to {0}".format(os.path.join(cfg.workdir, fn), p))
        if not os.path.isdir(cfg.workdir):
            os.makedirs(cfg.workdir)
        fileh = logging.handlers.RotatingFileHandler(os.path.join(cfg.workdir, fn), maxBytes=100e6, backupCount=10)
        fileh.setFormatter(formatter)
        args.preserve.append(fileh.stream)
        lg.addHandler(fileh)

    args.plugin.run(args)
