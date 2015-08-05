import os
import logging
import yaml
import multiprocessing

from lobster import cmssw, fs, se
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
    storage = se.StorageConfiguration(config['storage'])
    storage.activate()

    stats = dict((cfg['label'], [0, 0]) for cfg in config['tasks'])

    missing = []
    for cfg in config['tasks']:
        label = cfg['label']
        logger.info('validating output files for {0}'.format(label))

        files = set(fs.ls(label))
        delete = []

        for job, job_type in store.failed_jobs(label):
            for output in cfg['outputs']:
                base, ext = os.path.splitext(output)
                filename = os.path.join(
                        label,
                        cfg.get("output format", "{base}_{id}.{ext}").format(base=base, ext=ext[1:], id=job))

                if filename in files:
                    logger.info("found output from failed job: {0}".format(filename))
                    stats[label][0] += 1
                    if not args.dry_run:
                        delete.append(filename)

        for job, job_type in store.merged_jobs(label):
            for output in cfg['outputs']:
                base, ext = os.path.splitext(output)
                filename = os.path.join(
                        label,
                        cfg.get("output format", "{base}_{id}.{ext}").format(base=base, ext=ext[1:], id=job))

                if filename in files:
                    logger.info("found output from intermediate merged job: {0}".format(filename))
                    stats[label][1] += 1
                    if not args.dry_run and args.delete_merged:
                        delete.append(filename)

        for fn in delete:
            fs.remove(fn)

        for job, job_type in store.successful_jobs(label):
            for output in cfg['outputs']:
                base, ext = os.path.splitext(output)
                filename = os.path.join(
                        label,
                        cfg.get("output format", "{base}_{id}.{ext}").format(base=base, ext=ext[1:], id=job))

                if filename not in files:
                    missing.append(job)
                    logger.warning('output file is missing for {0}'.format(job))

    logger.info('finished validating')
    if sum(sum(stats.values(), [])) == 0:
        logger.info('no files found to cleanup')
    else:
        logger.info('{0:<20} {1:>20} {2:>20}'.format('label', '# of bad files', '# of merged files'))
        logger.info('-' * 62)
        for label, (fails, merges) in stats.items():
            if fails > 0 or merges > 0:
                logger.info('{0:<20} {1:>20} {2:>20}'.format(label, fails, merges))

        logger.info('-' * 62)
        logger.info('{0:<20} {1:>20} {2:>20}'.format('total', sum(f for f, m in stats.values()), sum(m for f, m in stats.values())))

    if len(missing) > 0:
        if not args.dry_run:
            store.update_missing(missing)

        verb = 'would have' if args.dry_run else 'have'
        template = 'the following {0} been marked as failed because their output could not be found: {1}'
        logger.warning(template.format(verb, ', '.join(map(str, missing))))

