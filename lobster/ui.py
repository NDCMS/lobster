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
    parser_run.add_argument('-i', '--bijective', action='store_true', default=False,
            help='use a 1-1 mapping for input and output files (process one input file per output file).')
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
    parser_cleanup.set_defaults(func=cleanup)

    parser_publish = subparsers.add_parser('publish', help='publish results for general consumption')
    parser_publish.add_argument('--block-size', dest='block_size', type=int, default=400,
            help='number of files to publish per file block.')
    parser_publish.add_argument('--clean', action='store_true',
            help='remove output files for failed jobs.')
    parser_publish.add_argument('labels', nargs='*', help='tasks to publish')
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
