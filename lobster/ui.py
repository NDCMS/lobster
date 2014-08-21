from argparse import ArgumentParser
import os

from lobster.cmssw.plotting import plot
from lobster.cmssw.publish import publish
from lobster.core import kill, cleanup, run

def boil():
    parser = ArgumentParser(description='A job submission tool for CMS')
    subparsers = parser.add_subparsers(title='commands')

    parser_run = subparsers.add_parser('process', help='process configuration')
    parser_run.add_argument('-f', '--foreground', action='store_true', default=False,
            help='do not daemonize;  run in the foreground instead')
    parser_run.set_defaults(merge=False)
    parser_run.set_defaults(func=run)

    parser_kill = subparsers.add_parser('terminate', help='terminate running lobster instance')
    parser_kill.set_defaults(func=kill)

    parser_plot = subparsers.add_parser('plot', help='plot progress of processing')
    parser_plot.add_argument("--from", default=None, metavar="START", dest="xmin",
            help="plot data from START.  Valid values: 1970-01-01, 1970-01-01_00:00, 00:00")
    parser_plot.add_argument("--to", default=None, metavar="END", dest="xmax",
            help="plot data until END.  Valid values: 1970-01-01, 1970-01-01_00:00, 00:00")
    parser_plot.add_argument('--outdir', help="specify output directory")
    parser_plot.set_defaults(func=plot)

    parser_cleanup = subparsers.add_parser('cleanup', help='remove output files for failed jobs')
    parser_cleanup.add_argument('--dry-run', action='store_true', dest='dry_run', default=False, help='only print (do not remove) files to be cleaned')
    parser_cleanup.set_defaults(func=cleanup)

    parser_publish = subparsers.add_parser('publish', help='publish results in the CMS Data Aggregation System')
    parser_publish.add_argument('--migrate-parents', dest='migrate_parents', default=False, help='migrate parents to local DBS')
    parser_publish.add_argument('--block-size', dest='block_size', type=int, default=400,
            help='number of files to publish per file block.')
    parser_publish.add_argument('labels', nargs='*', help='tasks to publish')
    parser_publish.set_defaults(func=publish)

    parser_merge = subparsers.add_parser('merge', help='merge output files into larger files')
    parser_merge.add_argument('--max-megabytes', dest='max_megabytes', type=float, default=3500, help='maximum merged file size')
    parser_merge.add_argument('--server', metavar="SERVER:<port>", default=None, help='override stageout server in configuration')
    parser_merge.set_defaults(foreground=False)
    parser_merge.set_defaults(merge=True)
    parser_merge.set_defaults(func=run)

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
