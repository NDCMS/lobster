import glob
import hadoopy
import os

def fs_handler(backup, *args, **kwargs):
    def wrapper(func):
        def call(path):
            if path.startswith('/hadoop'):
                return backup(path.replace('/hadoop', '', 1), *args, **kwargs)
            else:
                return func(path)
        return call
    return wrapper

@fs_handler(hadoopy.exists)
def exists(path):
    return os.path.exists(path)

@fs_handler(hadoopy.isdir)
def isdir(path):
    return os.path.isdir(path)

@fs_handler(hadoopy.mkdir)
def makedirs(path):
    return os.makedirs(path)

@fs_handler(hadoopy.stat, '%b')
def getsize(path):
    return os.path.getsize(path)

def isfile(path):
    if path.startswith('/hadoop'):
        return hadoopy.stat(path.replace('/hadoop', '', 1), '%F') == 'regular file'
    else:
        return os.path.isfile(path)

def ls(path):
    res = glob.glob(path)

    if len(res) == 0:
        try:
            res = ['/hadoop' + x for x in hadoopy.ls(path.replace('/hadoop', '', 1))]
        except:
            raise

    return res


