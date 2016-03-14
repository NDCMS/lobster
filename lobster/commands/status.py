import logging
import os
from lobster import util
from lobster.core import unit
from lobster.core.command import Command

class Status(Command):
    @property
    def help(self):
        return 'show a workflow status summary'

    def setup(self, argparser):
        pass

    def run(self, args):
        config = args.config
        logger = logging.getLogger('lobster.status')
        store = unit.UnitStore(config)
        data = list(store.workflow_status())

        widths = \
                [max(map(len, (xs[0] for xs in data)))] + \
                [max(map(len, (str(xs[i]) for xs in data))) for i in range(1, len(data[0]))]
        data.insert(1, ['=' * w for w in widths])
        headfmt = ' '.join('{{:^{0}}}'.format(w) for w in widths)
        mainfmt = '{{:{0}}} '.format(widths[0]) + ' '.join('{{:>{0}}}'.format(w) for w in widths[1:])
        report = \
                headfmt.format(*data[0]) + '\n' + \
                '\n'.join([mainfmt.format(*map(str, row)) for row in data[1:]])

        logger.info("workflow summary:\n" + report)

        wdir = config.workdir
        for wflow in config.workflows:
            tasks = store.failed_units(wflow.label)
            files = store.skipped_files(wflow.label)

            if len(tasks) > 0:
                msg = "tasks with failed units for {0}:".format(wflow.label)
                for task in tasks:
                    tdir = os.path.normpath(os.path.join(wdir, wflow.label, 'failed', util.id2dir(task)))
                    msg += "\n" + tdir
                logger.info(msg)

            if len(files) > 0:
                msg = "files skipped for {0}:\n".format(wflow.label) + "\n".join(files)
                logger.info(msg)
