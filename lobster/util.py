# vim: set fileencoding=utf-8 :

import collections
import logging
import os
import shutil
import subprocess
import yaml

from lockfile.pidlockfile import PIDLockFile
from lockfile import AlreadyLocked
from pkg_resources import get_distribution

logger = logging.getLogger('lobster.util')

def record(cls, *fields, **defaults):
    """
    Returns a class which is reminiscent of a namedtuple, except
    that it is mutable and accepts default values as keyword
    arguments. The optional `default` argument sets a
    universal default.

    >>> Point = record('Point', 'x', 'y', 'z')
    >>> p = Point(x=6, y=28, z=496)
    >>> p
    Point(x=6, y=28, z=496)
    >>> Point = record('Point', 'x', 'y', 'z', x=1, default=3.14)
    >>> p = Point()
    >>> p
    Point(x=1, y=3.14, z=3.14)
    >>> p.sql_fragment()
    'x=?, y=?, z=?'

    """

    class Record(collections.MutableSequence):
        def __init__(self, *args, **kwargs):
            if 'default' in defaults:
                for field in fields:
                    setattr(self, field, defaults['default'])
            for field, value in defaults.items():
                setattr(self, field, value)
            for field, value in kwargs.items():
                setattr(self, field, value)
            for field, value in zip(fields, args):
                setattr(self, field, value)

        def __len__(self):
            return len(fields)

        def __getitem__(self, index):
            return getattr(self, fields[index])

        def __setitem__(self, index, value):
            setattr(self, fields[index], value)

        def __delitem__(self, position):
            raise NotImplementedError

        def __repr__(self):
            descriptions = ['{0}={1}'.format(f, getattr(self, f)) for f in fields]
            return '{0}({1})'.format(cls, ', '.join(descriptions))

        def insert(self, index, value):
            self[index] = value

        @classmethod
        def sql_fragment(self, start=0, stop=len(fields)):
            return ', '.join(['{0}=?'.format(f) for f in fields[start:stop]])

    return Record

def id2dir(id):
    # Currently known limitations on the number of entries in a
    # sub-directory concern ext3, where said limit is 32k.  Use a
    # modus of 10k to split the task numbers.  Famous last words:
    # "(10k)² tasks should be enough for everyone." → we use two levels
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
    stored_version = checkpoint(workdir, 'version')
    if stored_version != my_version:
        raise ValueError, "Lobster {0!r} cannot process a run created with version {1!r}".format(my_version, stored_version)

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

def verify_string(s):
    try:
        s.decode('ascii')
    except (UnicodeDecodeError, AttributeError):
        return ""
    return s

def ldd(name):
    """Find libcrypto and libssl that `name` is linked to.

    CMS directories are excluded from the `LD_LIBRARY_PATH` while
    looking for libraries.  This is not guaranteed to work with ldd.

    Was used to ship compatibility libcrypto and libssl from RH5 to RH6
    systems, which don't install these versions by default.
    """
    libs = []

    env = dict(os.environ)

    def anti_cms_filter(d):
        return not (d.startswith('/cvmfs') or 'cms' in d)

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

def get_lock(workdir, force=False):
    pidfile = PIDLockFile(os.path.join(workdir, 'lobster.pid'), timeout=-1)
    try:
        pidfile.acquire()
    except AlreadyLocked:
        if not force:
            logger.error("another instance of lobster is accessing {0}".format(workdir))
            raise
    pidfile.break_lock()
    return pidfile

def taskdir(workdir, taskid, status='running'):
    tdir = os.path.normpath(os.path.join(workdir, status, id2dir(taskid)))
    if not os.path.isdir(tdir):
        os.makedirs(tdir)
    return tdir

def move(workdir, taskid, status, oldstatus='running'):
    """Moves a task parameter/log directory from one status directory to
    another.

    Returns the new directory.
    """
    # See above for task id splitting.  Moves directories and removes
    # old empty directories.
    old = os.path.normpath(os.path.join(workdir, oldstatus, id2dir(taskid)))
    new = os.path.normpath(os.path.join(workdir, status, id2dir(taskid)))
    parent = os.path.dirname(new)
    if not os.path.isdir(parent):
        os.makedirs(parent)
    shutil.move(old, parent)
    if len(os.listdir(os.path.dirname(old))) == 0:
        os.removedirs(os.path.dirname(old))
    return new
