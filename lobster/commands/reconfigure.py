import logging
import os

def reconfigure(args):
    config = args.config
    logger = logging.getLogger('lobster.reconfigure')

    cmd = 'config.{} = {}'.format(args.setting, args.value)
    logger.info("sending command: " + cmd)
    logger.info("check the log of the main process for success")

    icp = open(os.path.join(config.workdir, 'ipc'), 'w')
    icp.write(cmd)
