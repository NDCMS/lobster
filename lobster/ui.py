from argparse import ArgumentParser
import logging
import os
import sys
import yaml

# FIXME pycurl shipping with CMSSW is too old to harmonize with modern DBS!
rm = []
for f in sys.path:
    if '/cvmfs' in f and 'pycurl' in f:
        rm.append(f)
for f in rm:
    sys.path.remove(f)

from lobster.cmssw.publish import publish
from lobster.commands.process import kill, run
from lobster.commands.plot import plot
from lobster.commands.status import status
from lobster.commands.validate import validate
from lobster.core import config
from lobster import util

logger = logging.getLogger('lobster')

def boil():
    parser = ArgumentParser(description='A task submission tool for CMS')
    parser.add_argument('--verbose', '-v', action='count', default=0, help='increase verbosity')
    parser.add_argument('--quiet', '-q', action='count', default=0, help='decrease verbosity')
    subparsers = parser.add_subparsers(title='commands')

    parser_run = subparsers.add_parser('process', help='process configuration')
    parser_run.add_argument('--finalize', action='store_true', default=False,
            help='do not process any additional data; wrap project up by merging everything')
    parser_run.add_argument('--increase-thresholds', const=10, nargs='?', type=int,
            help='increase failure/skipping thresholds')
    parser_run.add_argument('--foreground', action='store_true', default=False,
            help='do not daemonize; run in the foreground instead')
    parser_run.add_argument('-f', '--force', action='store_true', default=False,
            help='force processing, even if the working directory is locked by a previous instance')
    parser_run.set_defaults(func=run)

    parser_kill = subparsers.add_parser('terminate', help='terminate running lobster instance')
    parser_kill.set_defaults(func=kill)

    parser_plot = subparsers.add_parser('plot', help='plot progress of processing')
    parser_plot.add_argument("--from", default=None, metavar="START", dest="xmin",
            help="plot data from START.  Valid values: 1970-01-01, 1970-01-01_00:00, 00:00")
    parser_plot.add_argument("--to", default=None, metavar="END", dest="xmax",
            help="plot data until END.  Valid values: 1970-01-01, 1970-01-01_00:00, 00:00")
    parser_plot.add_argument("--foreman-logs", default=None, metavar="FOREMAN_LIST", dest="foreman_list", nargs='+', type=str,
            help="specify log files for foremen;  valid values: log1 log2 log3...logN")
    parser_plot.add_argument('--outdir', help="specify output directory")
    parser_plot.set_defaults(func=plot)

    parser_validate = subparsers.add_parser('validate', help='validate task output and remove output files for failed tasks')
    parser_validate.add_argument('--dry-run', action='store_true', dest='dry_run', default=False,
            help='only print (do not remove) files to be cleaned')
    parser_validate.add_argument('--delete-merged', action='store_true', dest='delete_merged', default=False,
            help='remove intermediate files that have been merged')
    parser_validate.set_defaults(func=validate)

    parser_status = subparsers.add_parser('status', help='show a workflow status summary')
    parser_status.set_defaults(func=status)

    parser_publish = subparsers.add_parser('publish', help='publish results in the CMS Data Aggregation System')
    parser_publish.add_argument('--migrate-parents', dest='migrate_parents', default=False, help='migrate parents to local DBS')
    parser_publish.add_argument('--block-size', dest='block_size', type=int, default=400,
            help='number of files to publish per file block.')
    parser_publish.add_argument('datasets', nargs='*', help='dataset labels to publish (default is all datasets)')
    parser_publish.add_argument('-f', '--foreground', action='store_true', default=False,
            help='do not daemonize;  run in the foreground instead')
    parser_publish.set_defaults(func=publish)

    parser.add_argument(metavar='{configfile,workdir}', dest='checkpoint',
            help='configuration file to use or working directory to resume.')

    args = parser.parse_args()

    if os.path.isfile(args.checkpoint):
        configfile = args.checkpoint
        if configfile.endswith('.yaml') or configfile.endswith('.yml'):
            with open(configfile) as f:
                cfg = config.pythonize_yaml(yaml.load(f))
        else:
            import imp
            cfg = imp.load_source('userconfig', configfile).config

        if util.checkpoint(cfg.workdir, 'version'):
            cfg = config.Config.load(cfg.workdir)
        else:
            # This is the original configuration file!
            cfg.base_directory = os.path.abspath(os.path.dirname(configfile))
            cfg.base_configuration = os.path.abspath(configfile)
            cfg.startup_directory = os.path.abspath(os.getcwd())
    else:
        # Load configuration from working directory passed to us
        workdir = args.checkpoint
        try:
            cfg = config.Config.load(workdir)
        except:
            parser.error("the working directory '{0}' does not contain a valid configuration".format(workdir))
        cfg.workdir = workdir
    args.config = cfg

    # Handle logging for everything in only one place!
    level = max(1, args.config.advanced.log_level + args.quiet - args.verbose) * 10
    logger.setLevel(level)

    formatter = logging.Formatter(fmt='%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    logger.addHandler(console)

    if args.func in (run, publish):
        fn = ('process' if args.func == run else 'publish') + '.log'
        logger.info("saving log to {0}".format(os.path.join(cfg.workdir, fn)))
        if not os.path.isdir(cfg.workdir):
            os.makedirs(cfg.workdir)
        fileh = logging.handlers.RotatingFileHandler(os.path.join(cfg.workdir, fn), maxBytes=500e6, backupCount=10)
        fileh.setFormatter(formatter)
        args.preserve = fileh.stream
        logger.addHandler(fileh)

        if not args.foreground:
            logger.removeHandler(console)

        if args.func == run:
            if args.finalize:
                args.config.advanced.threshold_for_failure = 0
                args.config.advanced.threshold_for_skipping = 0
            if args.increase_thresholds:
                args.config.advanced.threshold_for_failure += args.increase_thresholds
                args.config.advanced.threshold_for_skipping += args.increase_thresholds
                args.config.save()

    args.func(args)
