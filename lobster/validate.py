import os
import logging
import yaml
import multiprocessing

from lobster import cmssw
from lobster.job import apply_matching

def validate(args):
    with open(args.configfile) as configfile:
        config = yaml.load(configfile)

    logger = multiprocessing.get_logger()

    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] - %(pathname)s %(lineno)d: %(message)s"))
    logger.addHandler(console)
    logger.setLevel(logging.INFO)

    store = cmssw.jobit.JobitStore(config)
    config = apply_matching(config)
    deleted_files = dict((cfg['label'], 0) for cfg in config['tasks'])
    deleted_sizes = dict((cfg['label'], 0) for cfg in config['tasks'])
    missing = []
    for cfg in config['tasks']:
        good_files = []
        label = cfg['label']
        logging.info('validating output files for {0}'.format(label))
        for job, merged_job in store.finished_jobs(label):
            for output in cfg['outputs']:
                output_format = cfg.get("output format", "{base}_{id}.{ext}")
                name = cmssw.merge.resolve_name(job, merged_job, output, output_format)
                if not os.path.isfile(os.path.join(config['stageout location'], label, name)):
                    update = [(job, merged_job)]
                    missing += update
                    logging.warning('output file is missing for {0}'.format(''.join(cmssw.merge.resolve_joblist(update))))
                else:
                    good_files += [name]

        if args.cleanup:
            for dirpath, dirnames, filenames in os.walk(os.path.join(config['stageout location'], label)):
                logging.info('looking for output files to cleanup in {0}'.format(label))

                for file in filenames:
                    if file not in good_files:
                        deleted_files[label] += 1
                        deleted_sizes[label] += os.path.getsize(os.path.join(dirpath, file))
                        if not args.dry_run:
                            os.remove(os.path.join(dirpath, file))

    if args.cleanup:
        logging.info('finished cleaning')
        if sum(deleted_files.values()) == 0:
            logging.info('no files found to cleanup')
        else:
            logging.info('%-20s %-20s %-20s' % ('label', 'number of bad files', 'total size [MB]'))
            for (label, files), size in zip(deleted_files.items(), deleted_sizes.values()):
                if files > 0:
                    logging.info('%-20s %-20i %-20i' % (label, files, size / 1000000))

            logging.info('%-20s %-20i %-20i' % ('total', sum(deleted_files.values()), sum(deleted_sizes.values()) / 1000000))

    if len(missing) > 0:
        if not args.dry_run:
            store.update_missing(missing)

        template = 'the following have been marked as failed because their output could not be found: {0}'
        logging.warning(template.format(cmssw.merge.resolve_joblist(missing)))

