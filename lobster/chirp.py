import multiprocessing
import os

logger = multiprocessing.get_logger()

def unlink(server, basedir, files):
    if os.isdir(basedir):
        # We have access to the stageout base directory
        for file in files:
            path = os.path.join(basedir, file)
            if os.path.isfile(path):
                logger.info("deleting " + path)
                os.unlink(path)
    else:
        cmd = "".join("rm " + file + "\n" for file in files)
        p = subprocess.Popen(
                ["chirp",server],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE)
        out, err = process.communicate(cmd)


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
