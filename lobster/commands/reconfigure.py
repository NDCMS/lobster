import logging
import os

from lobster import util
from lockfile import AlreadyLocked

def reconfigure(args):
    config = args.config
    logger = logging.getLogger('lobster.reconfigure')

    try:
        pidfile = util.get_lock(config.workdir)
        logger.info("Lobster process not running, directly changing configuration.")
        with util.PartiallyMutable.lockdown():
            cmd = 'config.{} = {}'.format(args.setting, args.value)
            exec cmd in {'config': config}, {}
            config.save()
    except AlreadyLocked:
        logger.info("Lobster process still running, contacting process...")
        cmd = 'config.{} = {}'.format(args.setting, args.value)
        logger.info("sending command: " + cmd)
        logger.info("check the log of the main process for success")

        icp = open(os.path.join(config.workdir, 'ipc'), 'w')
        icp.write(cmd)
    except Exception as e:
        logger.error("can't change values: {}".format(e))
