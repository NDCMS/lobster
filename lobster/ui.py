from argparse import ArgumentParser
import os

from lobster.cmssw.plotting import plot
from lobster.cmssw.publish import publish
from lobster.core import kill, run
from lobster.validate import validate

def boil():
    parser = ArgumentParser(description='A job submission tool for CMS')
    subparsers = parser.add_subparsers(title='commands')

    parser_run = subparsers.add_parser('process', help='process configuration')
    parser_run.add_argument('-f', '--foreground', action='store_true', default=False,
            help='do not daemonize;  run in the foreground instead')
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

    if os.path.isdir(args.checkpoint):
        configfile = os.path.join(args.checkpoint, 'lobster_config.yaml')
        if not os.path.isfile(configfile):
            parser.error('the working directory specified does not contain a configuration')
        args.configfile = os.path.abspath(configfile)
    else:
        args.configfile = os.path.abspath(args.checkpoint)

    args.configdir = os.path.dirname(args.configfile)
    args.startdir = os.getcwd()
    args.func(args)
