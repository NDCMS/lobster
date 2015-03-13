import multiprocessing
import os
import subprocess

logger = multiprocessing.get_logger()

def get_chirp_output(server, args=None, cmd=None, timeout=10, throw=False):
    if not args:
        args = []
    p = subprocess.Popen(
            ["chirp", "-t", str(timeout), server] + args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            stdin=subprocess.PIPE)
    out, err = p.communicate(cmd)

    logger.debug("args:\"{0}\"".format('" "'.join(args)))
    logger.debug("stdout:\n{0}".format(out))
    logger.debug("stderr:\n{0}".format(err))

    if throw and p.returncode != 0:
        raise subprocess.CalledProcessError(p.returncode,
                " ".join(["chirp", "-t", str(timeout), server] + args))

    return out

def exists(server, basedir, file):
    if os.path.isdir(basedir):
        # We have access to the stageout base directory
        path = os.path.join(basedir, file)
        return os.path.exists(path)
    elif server:
        out = get_chirp_output(server, args=["stat", file])
        return len(out.splitlines()) > 1
    else:
        raise IOError("Can't access stageout directory.")

def isfile(server, basedir, file):
    if os.path.isdir(basedir):
        # We have access to the stageout base directory
        path = os.path.join(basedir, file)
        return os.path.isfile(path)
    elif server:
        out = get_chirp_output(server, args=["stat", file])
        try:
            # no links means a directory
            if out.splitlines()[4].split() == ["nlink:", "0"]:
                return False
        except:
            return False
        out = get_chirp_output(server, args=["ls", "-la", file])
        return len(out) > 0 and not out.startswith('d')
    else:
        raise IOError("Can't access stageout directory.")

def listdir(server, basedir, dir):
    if os.path.isdir(basedir):
        for file in os.listdir(os.path.join(basedir, dir)):
            if os.path.isfile(os.path.join(basedir, dir, file)):
                yield os.path.join(dir, file)
    elif server:
        out = get_chirp_output(server, args=['ls', '-la', dir])
        for line in out.splitlines():
            if not line.startswith('d'):
                file = line.split(None, 9)[8]
                yield os.path.join(dir, file)
    else:
        raise IOError("Can't access stageout directory.")

def makedirs(server, basedir, dir):
    if dir.startswith(os.path.sep):
        dir = dir[len(os.path.sep):]
    if os.path.isdir(basedir):
        # We have access to the stageout base directory
        if not os.path.exists(os.path.join(basedir, dir)):
            os.makedirs(os.path.join(basedir, dir))
    elif server:
        get_chirp_output(server, args=['mkdir', '-p', dir])
    else:
        raise IOError("Can't access stageout directory.")

def unlink(server, basedir, files):
    if os.path.isdir(basedir):
        # We have access to the stageout base directory
        for file in files:
            path = os.path.join(basedir, file)
            if os.path.isfile(path):
                logger.info("deleting " + path)
                os.unlink(path)
    elif server:
        out = get_chirp_output(server, cmd="".join("rm " + file + "\n" for file in files))
    else:
        raise IOError("Can't access stageout directory.")


class Unlinker(object):
    def __init__(self, stageout, server):
        self.__stageout = stageout
        self.__server = server

        def unlinkf(q):
            args = q.get()
            while args not in ('stop', None):
                unlink(*args)
                args = q.get()

        self.__queue = multiprocessing.Queue()
        self.__process = multiprocessing.Process(target=unlinkf, args=(self.__queue,))
        self.__process.start()

    def __del__(self):
        self.__queue.put('stop')

    def remove(self, files):
        self.__queue.put((self.__server, self.__stageout, files))
