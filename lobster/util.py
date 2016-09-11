# vim: set fileencoding=utf-8 :

# Only use default packages here!  This package is included by `setup.py`.
# Since this happens *before* dependencies are installed, only packages
# included with the default python distribution should be imported at the
# top level.
#
# If optional packages are needed, they should be included in the function
# scope.

import collections
import inspect
import json
import logging
import os
import shlex
import shutil
import subprocess
import time

from contextlib import contextmanager
from pkg_resources import get_distribution

logger = logging.getLogger('lobster.util')


class PartiallyMutable(type):

    """Support metaclass for partially mutable base object.

    This metaclass makes sure that classes have an attribute `_mutable` and
    sets the attribute `_constructed` to `True` after an instance has been
    constructed.
    """
    _actions = set()

    def __init__(cls, name, bases, attrs):
        key = '_mutable'
        if key not in attrs:
            raise AttributeError('class {} does not set the attribute _mutable'.format(name))
        elif not isinstance(attrs[key], dict):
            raise AttributeError(
                'class {} does not define the attribute _mutable as dict'.format(name))
        if '__init__' in attrs:
            args = inspect.getargspec(cls.__init__).args
            for attr in attrs['_mutable']:
                if attr not in args:
                    raise AttributeError(
                        'class {} defines {} as mutable, but does not list it in the constructor'.format(name, attr))
        elif len(attrs['_mutable']) > 0:
            raise AttributeError(
                'class {} defines mutable attributes, but does not list them in the constructor'.format(name))
        type.__init__(cls, name, bases, attrs)

    def __call__(cls, *args, **kwargs):
        try:
            res = type.__call__(cls, *args, **kwargs)
            name = cls.__name__
            module = cls.__module__.split('.')[1]
            if module not in ('core', 'se'):
                name = ".".join([module, name])
            res._store(name, args, kwargs)
            res._constructed = True
            for arg in inspect.getargspec(res.__init__).args[1:]:
                if arg not in vars(res):
                    raise AttributeError(
                        'class {} uses {} in the constructor, but does define it as property'.format(name, arg))
        except Exception as e:
            import sys
            raise type(e), type(e)('{0!s}: {1}'.format(cls, e.message)), sys.exc_info()[2]
        return res

    @classmethod
    @contextmanager
    def unlock(cls):
        cls._fixed = False
        yield
        cls._fixed = True

    @classmethod
    def changes(cls):
        for tpl in cls._actions:
            yield tpl
        cls._actions.clear()

    @classmethod
    def purge(cls):
        cls._actions.clear()


class Configurable(object):

    """Partially mutable base object.

    Subclasses will have to define a class attribute `_mutable`, a
    dictionary linking all attributes to an action to be performed.
    Attributes are given as strings, while the action to be performed is a
    tuple consisting of the callback to be called, in the form of either
    `source.spam` or `config.foo.bar` to be changed, the arguments to be
    passed, and a bool indicating if the changed object should be appended
    to the arguments.
    """
    __metaclass__ = PartiallyMutable
    _mutable = {}

    def __setattr__(self, attr, value):
        if not getattr(self, '_constructed', False):
            super(Configurable, self).__setattr__(attr, value)
        elif attr in self._mutable or not getattr(PartiallyMutable, '_fixed', True):
            super(Configurable, self).__setattr__(attr, value)
            if attr in self._mutable and getattr(PartiallyMutable, '_fixed', True):
                method, args, append = self._mutable[attr]
                # force a copy of the list into a tuple (for the actions,
                # since it's a set)
                if append:
                    args = tuple(args + [self])
                else:
                    args = tuple(args)
                self.__class__._actions.add((method, args))
        else:
            raise AttributeError("can't change attribute {} of type {}".format(attr, type(self)))

    def _store(self, name, args, kwargs):
        self.__name = name
        self.__args = args
        self.__kwargs = kwargs

    def __repr__(self, override=None):
        argspec = inspect.getargspec(self.__init__)
        defaults = dict(zip(reversed(argspec.args), reversed(argspec.defaults)))
        # Look for altered mutable properties, add them to constructor
        # arguments
        for arg in self._mutable:
            arg = arg
            if getattr(self, arg) != defaults.get(arg):
                self.__kwargs[arg] = getattr(self, arg)
            elif arg in self.__kwargs:
                del self.__kwargs[arg]

        def indent(text):
            lines = text.splitlines()
            if len(lines) <= 1:
                return text
            return "\n".join("    " + l for l in lines).strip()

        def attr(k):
            if override and k in override:
                return override[k]
            elif not hasattr(self, k):
                return repr(getattr(self, '_{}__{}'.format(self.__class__.__name__, k)))
            return repr(getattr(self, k))
        args = ["\n    {},".format(indent(a)) for a in self.__args]
        kwargs = ["\n    {}={}".format(k, indent(attr(k)))
                  for k, v in sorted(self.__kwargs.items(), key=lambda (x, y): x)]
        s = self.__name + "({}\n)".format(",".join(args + kwargs))
        return s

    def update(self, other):
        logger = logging.getLogger('lobster.configure')

        if not isinstance(other, type(self)):
            logger.error("can't compare {} and {}".format(type(self), type(other)))
            return
        argspec = inspect.getargspec(self.__init__)
        for arg in argspec.args[1:]:
            ours = getattr(self, arg)
            our_original = self.__kwargs.get(arg, None)
            theirs = getattr(other, arg)

            if isinstance(ours, Configurable):
                ours.update(theirs)
            elif hasattr(ours, '__iter__') or hasattr(theirs, '__iter__'):
                # protect against empty default lists
                if ours is None:
                    ours = []
                if theirs is None:
                    theirs = []
                if our_original is None:
                    our_original = []

                diff = len(ours) - len(theirs)
                if diff != 0 and arg not in self._mutable:
                    if our_original != theirs:
                        logger.error("modified immutable list {}".format(arg))
                    else:
                        logger.warning("skipping immutable list {}".format(arg))
                    continue
                elif diff > 0:
                    logger.info(
                        "truncating list '{}' by removing elements {}".format(arg, ours[-diff:]))
                    ours = ours[:-diff]
                    setattr(self, arg, ours)
                elif diff < 0:
                    logger.info(
                        "expanding list '{}' by adding elements {}".format(arg, theirs[diff:]))
                    ours += theirs[diff:]
                    setattr(self, arg, ours)

                changed = False
                for n in range(len(ours)):
                    if hasattr(ours[n], '__iter__'):
                        logger.error("nested list in attribute '{}' not supported".format(arg))
                        continue
                    elif isinstance(ours[n], Configurable):
                        ours[n].update(theirs[n])
                    elif ours[n] != theirs[n]:
                        if our_original != theirs and arg not in self._mutable:
                            logger.error("modified item in immutable list '{}'".format(arg))
                            continue
                        logger.info(
                            "updating item {} of list '{}' with value '{}' (old: '{}')".format(n, arg, theirs[n], ours[n]))
                        ours[n] = theirs[n]
                        changed = True
                if changed:
                    setattr(self, arg, ours)
            elif ours != theirs and our_original != theirs:
                if arg not in self._mutable:
                    logger.error("can't change value of immutable attribute '{}'".format(arg))
                    continue
                logger.info(
                    "updating attribute '{}' with value '{}' (old: '{}')".format(arg, theirs, ours))
                setattr(self, arg, theirs)


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


class Timing(object):

    """
    Baseclass to simplify keeping track of the timing of things.
    """

    def __init__(self, *keys):
        self._times = {k: 0 for k in keys}

    @property
    def times(self):
        return dict(self._times)

    @contextmanager
    def measure(self, what):
        t = time.time()
        yield
        self._times[what] += int((time.time() - t) * 1e6)


def id2dir(id):
    # Currently known limitations on the number of entries in a
    # sub-directory concern ext3, where said limit is 32k.  Use a
    # modus of 10k to split the task numbers.  Famous last words:
    # "(10k)^2 tasks should be enough for everyone." -> we use two levels
    # only.
    id = int(id)
    man = str(id % 10000).zfill(4)
    oku = str(id / 10000).zfill(4)
    return os.path.join(oku, man)


def findpath(dirs, path):
    if len(dirs) == 0:
        return path

    if os.path.isabs(path):
        return path

    for directory in dirs:
        joined = os.path.join(directory, path)
        if os.path.exists(joined):
            return joined
    raise KeyError("Can't find '{0}' in {1}".format(path, dirs))


def which(name):
    paths = os.getenv('PATH')
    for path in paths.split(os.path.pathsep):
        exe = os.path.join(path, name)
        if os.path.exists(exe) and os.access(exe, os.F_OK | os.X_OK):
            return exe
    raise KeyError("Can't find '{0}' in PATH".format(name))


def verify(workdir):
    if not os.path.exists(workdir):
        return

    my_version = get_version()
    major, head, status = my_version.split('-')
    my_version = major

    stored_version = checkpoint(workdir, 'version')
    major, head, status = stored_version.split('-')
    stored_version = major

    if stored_version != my_version:
        raise ValueError("Lobster {0!r} cannot process a run created with version {1!r}".format(
            my_version, stored_version))


def checkpoint(workdir, key):
    statusfile = os.path.join(workdir, 'status.json')
    if os.path.exists(statusfile):
        with open(statusfile, 'r') as f:
            s = json.load(f)
            return s.get(key)


def register_checkpoint(workdir, key, value):
    statusfile = os.path.join(workdir, 'status.json')
    if not os.path.exists(statusfile):
        with open(statusfile, 'w') as f:
            json.dump({key: value}, f)
    else:
        with open(statusfile, 'r') as f:
            s = json.load(f)
            s[key] = value
        with open(statusfile, 'w') as f:
            f.write(json.dumps(s, sort_keys=True, indent=4))


def get_version():
    if 'site-packages' in __file__:
        version = get_distribution('Lobster').version
    else:
        start = os.getcwd()
        os.chdir(os.path.dirname(__file__))
        head = subprocess.check_output(shlex.split('git rev-parse --short HEAD')).strip()
        diff = subprocess.check_output(shlex.split('git diff'))
        status = 'dirty' if diff else 'clean'
        os.chdir(start)
        version = '{major}-{head}-{status}'.format(major=1.5, head=head, status=status)
    return version


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
    from lockfile.pidlockfile import PIDLockFile
    from lockfile import AlreadyLocked

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
