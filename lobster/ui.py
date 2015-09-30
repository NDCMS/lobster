from argparse import ArgumentParser
import os
import yaml

from lobster.cmssw.plotting import plot
from lobster.cmssw.publish import publish
from lobster.core import kill, run
from lobster.validate import validate
from lobster import util

def boil():
    parser = ArgumentParser(description='A job submission tool for CMS')
    parser.add_argument('--verbose', '-v', action='count', default=0, help='increase verbosity')
    parser.add_argument('--quiet', '-q', action='count', default=0, help='decrease verbosity')
    subparsers = parser.add_subparsers(title='commands')

    parser_run = subparsers.add_parser('process', help='process configuration')
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

    parser_validate = subparsers.add_parser('validate', help='validate job output and remove output files for failed jobs')
    parser_validate.add_argument('--dry-run', action='store_true', dest='dry_run', default=False,
            help='only print (do not remove) files to be cleaned')
    parser_validate.add_argument('--delete-merged', action='store_true', dest='delete_merged', default=False,
            help='remove intermediate files that have been merged')
    parser_validate.set_defaults(func=validate)

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
        if util.checkpoint(os.path.dirname(configfile), 'version'):
            # If we are resuming, the working directory might have been moved.
            # Thus check checkpoint of configfile directory!
            workdir = os.path.dirname(configfile)
        else:
            # Otherwise load the working directory from the configuration
            # and use the configuration file stored there (if available)
            with open(configfile) as f:
                workdir = yaml.load(f)['workdir']
            fn = os.path.join(workdir, 'lobster_config.yaml')
            if os.path.isdir(workdir) and os.path.isfile(fn):
                configfile = fn
    else:
        # Load configuration from working directory passed to us
        workdir = args.checkpoint
        configfile = os.path.join(workdir, 'lobster_config.yaml')
        if not os.path.isfile(configfile):
            parser.error("the working directory '{0}' does not contain a configuration".format(workdir))

    with open(configfile) as f:
        args.config = yaml.load(f)
    args.config['workdir'] = workdir

    if configfile == args.checkpoint:
        # This is the original configuration file!
        args.config['base directory'] = os.path.dirname(configfile)
        args.config['base configuration'] = configfile
        args.config['startup directory'] = os.getcwd()

    args.func(args)
