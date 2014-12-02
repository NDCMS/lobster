import multiprocessing
import os
import subprocess

logger = multiprocessing.get_logger()

def listdir(server, basedir, dir):
    if os.path.isdir(basedir):
        for file in os.listdir(os.path.join(basedir, dir)):
            if os.path.isfile(os.path.join(basedir, dir, file)):
                yield os.path.join(dir, file)
    elif server:
        p = subprocess.Popen(
                ['chirp', server, 'ls', '-la', dir],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
        out, err = p.communicate()
        for line in out.splitlines():
            if not line.startswith('d'):
                file = line.split(None, 9)[8]
                yield os.path.join(dir, file)
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
        cmd = "".join("rm " + file + "\n" for file in files)
        p = subprocess.Popen(
                ["chirp",server],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE)
        out, err = p.communicate(cmd)
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

    def __del__(self):
        self.__queue.put('stop')

    def remove(self, files):
        self.__queue.put((self.__server, self.__stageout, files))
