import logging

from lobster import fs
from lobster.core.command import Command
from lobster.core.unit import UnitStore


logger = logging.getLogger('lobster.validate')


class Validate(Command):

    @property
    def help(self):
        return 'validate task output and remove output files for failed tasks'

    def setup(self, argparser):
        argparser.add_argument('--dry-run', action='store_true', dest='dry_run', default=False,
                               help='only print (do not remove) files to be cleaned')
        argparser.add_argument('--delete-merged', action='store_true', dest='delete_merged', default=False,
                               help='remove intermediate files that have been merged')

    def print_stats(self, stats):
        width = max([len(x) for x in stats])
        logger.info('{0:<{width}} {1:>20} {2:>20} {3:>23}'.format('label',
                                                             '# of bad files',
                                                             '# of merged files',
                                                             '# of uncleaned files',
                                                             width=width))
        logger.info('-' * (66 + width))
        for label, (fails, merges, uncleaned) in stats.items():
            if fails > 0 or merges > 0 or uncleaned > 0:
                logger.info('{0:<{width}} {1:>20} {2:>20} {3:>23}'.format(label,
                                                                          fails,
                                                                          merges,
                                                                          uncleaned,
                                                                          width=width))

        logger.info('-' * (66 + width))
        logger.info('{0:<{width}} {1:>20} {2:>20} {3:>23}'.format('total',
                                                                  sum(f for f, m, c in stats.values()),
                                                                  sum(m for f, m, c in stats.values()),
                                                                  sum(c for f, m, c in stats.values()),
                                                                  width=width))

    def process_workflow(self, store, stats, wflow):
        files = set(fs.ls(wflow.label))
        delete = []
        merged = []
        missing = []

        cleaned = any(w.cleanup_input for w in wflow.dependents)

        for task, task_type in store.failed_tasks(wflow.label):
            for _, filename in wflow.get_outputs(task):
                if filename in files:
                    logger.info("found output from failed task: {0}".format(filename))
                    stats[wflow.label][0] += 1
                    delete.append(filename)

        for task, task_type in store.merged_tasks(wflow.label):
            for _, filename in wflow.get_outputs(task):
                if filename in files:
                    logger.info("found output from intermediate merged task: {0}".format(filename))
                    stats[wflow.label][1] += 1
                    merged.append(filename)

        if cleaned:
            for w in wflow.dependents:
                if not w.cleanup_input:
                    continue
                if store.unfinished_units(w.label) == 0:
                    for fn in files:
                        logger.warning('found output from tasks that should have been cleaned up: {}'.format(fn))
                    stats[wflow.label][2] += len(files)
                    delete.extend(files)
            else:
                logger.error("can't validate workflow {}, as its dependents have not completed and cleaned it up".format(wflow.label))
        else:
            for task, task_type in store.successful_tasks(wflow.label):
                for _, filename in wflow.get_outputs(task):
                    if filename not in files:
                        missing.append(task)
                        logger.warning('output file is missing for {0}'.format(task))

        return delete, merged, missing

    def run(self, args):
        store = UnitStore(args.config)
        stats = dict((w.label, [0, 0, 0]) for w in args.config.workflows)

        missing = []
        for wflow in args.config.workflows:
            logger.info('validating output files for {0}'.format(wflow.label))

            delete, merged, missed = self.process_workflow(store, stats, wflow)
            missing += missed

            if not args.dry_run:
                fs.remove(*delete)
                if args.delete_merged:
                    fs.remove(*merged)

        logger.info('finished validating')

        if sum(sum(stats.values(), [])) == 0:
            logger.info('no files found to cleanup')
        else:
            self.print_stats(stats)

        if len(missing) > 0:
            if not args.dry_run:
                store.update_missing(missing)

            verb = 'would have' if args.dry_run else 'have'
            template = 'the following {0} been marked as failed because their output could not be found: {1}'
            logger.warning(template.format(verb, ', '.join(map(str, missing))))
