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

    config = apply_matching(config)
    store = cmssw.jobit.JobitStore(config)

    deleted_files = dict((cfg['label'], 0) for cfg in config['tasks'])
    deleted_sizes = dict((cfg['label'], 0) for cfg in config['tasks'])
    missing = []
    for cfg in config['tasks']:
        label = cfg['label']
        logger.info('validating output files for {0}'.format(label))

        for job, job_type in store.success_jobs(label):
            for output in cfg['outputs']:
                output_format = cfg.get("output format", "{base}_{id}.{ext}")
                name = cmssw.merge.resolve_name(job, job_type, output, output_format)

                if not os.path.isfile(os.path.join(config['stageout location'], label, name)):
                    missing += [(job, job_type)]
                    logger.warning('output file is missing for {0}'.format(job))

        for job, job_type in store.fail_jobs(label):
            for output in cfg['outputs']:
                output_format = cfg.get("output format", "{base}_{id}.{ext}")
                name = cmssw.merge.resolve_name(job, job_type, output, output_format)
                path = os.path.join(config['stageout location'], label, name)

                if os.path.isfile(path):
                    logger.info("found output from failed job: {0}".format(path))
                    deleted_files[label] += 1
                    deleted_sizes[label] += os.path.getsize(path)

                    if not args.dry_run:
                        try:
                            os.remove(path)
                        except Exception as e:
                            logger.critical("error removing {0}.format(os.path.join(dirpath, file))")
                            logger.critical(e)

    logger.info('finished validating')
    if sum(deleted_files.values()) == 0:
        logger.info('no files found to cleanup')
    else:
        logger.info('%-20s %-20s %-20s' % ('label', 'number of bad files', 'total size [MB]'))
        for (label, files), size in zip(deleted_files.items(), deleted_sizes.values()):
            if files > 0:
                logger.info('%-20s %-20i %-20i' % (label, files, size / 1000000))

        logger.info('%-20s %-20i %-20i' % ('total', sum(deleted_files.values()), sum(deleted_sizes.values()) / 1000000))

    if len(missing) > 0:
        if not args.dry_run:
            store.update_missing(missing)

        verb = 'would have' if args.dry_run else 'have'
        template = 'the following {0} been marked as failed because their output could not be found: {1}'
        logger.warning(template.format(verb, cmssw.merge.resolve_joblist(missing)))

