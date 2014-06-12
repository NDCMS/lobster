import os
import yaml

def findpath(dirs, path):
    if len(dirs) == 0:
        return path

    for directory in dirs:
        joined = os.path.join(directory, path)
        if os.path.exists(joined):
            return joined
    raise KeyError, "Can't find '{0}' in {1}".format(path, dirs)

def which(name):
    paths = os.getenv('PATH')
    for path in paths.split(os.path.pathsep):
        exe = os.path.join(path, name)
        if os.path.exists(exe) and os.access(exe, os.F_OK|os.X_OK):
            return exe
    raise KeyError, "Can't find '{0}' in PATH".format(name)

def checkpoint(workdir, key):
    statusfile = os.path.join(workdir, 'status.yaml')
    if os.path.exists(statusfile):
        with open(statusfile, 'rb') as f:
            s = yaml.load(f)
            return s.get(key)
    else:
        return False

def register_checkpoint(workdir, key, value):
    statusfile = os.path.join(workdir, 'status.yaml')
    with open(statusfile, 'a') as f:
        yaml.dump({key: value}, f, default_flow_style=False)

