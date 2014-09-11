# vim: set fileencoding=utf-8 :

import os
import yaml
import subprocess

from pkg_resources import get_distribution

def id2dir(id):
    # Currently known limitations on the number of entries in a
    # sub-directory concern ext3, where said limit is 32k.  Use a
    # modus of 10k to split the job numbers.  Famous last words:
    # "(10k)² jobs should be enough for everyone." → we use two levels
    # only.
    id = int(id)
    man = str(id % 10000).zfill(4)
    oku = str(id / 10000).zfill(4)
    return os.path.join(oku, man)

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

def verify(workdir):
    if not os.path.exists(workdir):
        return

    my_version = get_distribution('Lobster').version
    stored_version = checkpoint(workdir, 'VERSION')
    if stored_version != my_version:
        raise ValueError, "Lobster {0} cannot process a run created with version {1}".format(my_version, stored_version)

def checkpoint(workdir, key):
    statusfile = os.path.join(workdir, 'status.yaml')
    if os.path.exists(statusfile):
        with open(statusfile, 'rb') as f:
            s = yaml.load(f)
            return s.get(key)

def register_checkpoint(workdir, key, value):
    statusfile = os.path.join(workdir, 'status.yaml')
    with open(statusfile, 'a') as f:
        yaml.dump({key: value}, f, default_flow_style=False)

def ldd(name):
    libs = []

    env = dict(os.environ)

    def anti_cms_filter(d):
        return not (d.startswith('/cvmfs') or 'grid' in d or 'cms' in d)

    env["LD_LIBRARY_PATH"] = os.path.pathsep.join(
            filter(anti_cms_filter, os.environ.get("LD_LIBRARY_PATH", "").split(os.path.pathsep)))
    env["PATH"] = os.path.pathsep.join(
            filter(anti_cms_filter, os.environ.get("PATH", "").split(os.path.pathsep)))

    p = subprocess.Popen(["ldd", which(name)], env=env,
            stdout=subprocess.PIPE)
    out, err = p.communicate()

    for line in out.splitlines():
        fields = line.split()

        if len(fields) < 3 or fields[1] != "=>":
            continue

        lib = fields[0]
        target = fields[2]

        if lib.startswith('libssl') or lib.startswith('libcrypto'):
            libs.append(target)

    return libs
