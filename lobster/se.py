import glob
import hadoopy
import os
import re
import subprocess

from functools import partial

class FileSystem(object):
    _dispatch = {}
    _default = None

    def __init__(self, prefix=None, mode='', default=False):
        """Create or use a FileSystem abstraction.

        As a user, use with no parameters to access various storage
        elements transparently.

        Subclasses should call the constructor with appropriate arguments,
        which should also be made available to the user.

        Parameters
        ----------
        prefix : string, optional
            Specifies the prefix for which the implementation is chosen.
            If `None`, acts as abstraction that switches implementations
            based on method parameters.
        mode : {'', 'a', 'r'}
            Specifies how to handle the prefix.  The default, `''`, does
            not alter the path.  With `'a'`, the `prefix` gets prepended
            before operations and removed from results, with `'r'`, the
            opposite:  the `prefix` is removed before operations and
            prepended to results.
        default : bool
            Specifies a default system.
        """
        self.__master = False

        if mode not in ['', 'a', 'r']:
            raise TypeError("mode '{0}' not in ('', 'a', 'r')".format(mode))

        if prefix is not None:
            if default:
                if FileSystem._default:
                    raise AttributeError('Default file system defined twice.')
                FileSystem._default = self
            else:
                FileSystem._dispatch[prefix] = self
            self._prefix = prefix
            self._mode = mode
            self._absolute = False
        else:
            self.__master = True

    def __getattr__(self, attr):
        if attr in self.__dict__ or not self.__master:
            return self.__dict__[attr]

        def switch(path):
            print path
            imp = self._default
            for k, o in self._dispatch.items():
                if path.startswith(k):
                    imp = o
            print imp
            # print imp._mode
            print imp._prefix
            return imp.fixresult(getattr(imp, attr)(imp.fixpath(path)))
        return switch

    def fixpath(self, path):
        if self._mode == '':
            return path

        if self._mode == 'a':
            return self._prefix + path

        localpath = path.replace(self._prefix, '', 1)
        if self._prefix.endswith('/') and self._absolute:
            localpath = '/' + localpath
        return localpath

    def fixresult(self, res):
        if self._mode == '':
            return res

        if self._mode == 'a':
            def fixup(p):
                return p.replace(self._prefix, 1)
        else:
            def fixup(p):
                if self._prefix.endswith('/') and self._absolute:
                    p = p[1:]
                return self._prefix + p

        if isinstance(res, str):
            return fixup(res)

        try:
            return map(fixup, res)
        except TypeError:
            return res

    @classmethod
    def reset(cls):
        cls._dispatch = {}
        cls._default = None

class Local(FileSystem):
    def __init__(self, prefix='', mode='', default=True):
        super(Local, self).__init__(prefix=prefix, mode=mode, default=default)
        self.exists = os.path.exists
        self.getsize = os.path.getsize
        self.isdir = os.path.isdir
        self.isfile = os.path.isfile
        self.ls = glob.glob
        self.makedirs = os.makedirs
        self.remove = os.remove

class Hadoop(FileSystem):
    def __init__(self, prefix='/hadoop/', mode='r', default=False):
        super(Hadoop, self).__init__(prefix=prefix, mode=mode, default=default)
        self._absolute = True

        self.exists = hadoopy.exists
        self.getsize = partial(hadoopy.stat, format='%b')
        self.isdir = hadoopy.isdir
        self.isfile = os.path.isfile
        self.ls = hadoopy.ls
        self.makedirs = hadoopy.mkdir
        self.remove = hadoopy.rmr

        # local imports are not available after the module hack at the end
        # of the file
        self.__hadoop = hadoopy

    def isfile(self, path):
        return self.__hadoop.stat(path, '%F') == 'regular file'

class SRM(FileSystem):
    def __init__(self, prefix='srm://', mode='', default=False):
        super(SRM, self).__init__(prefix=prefix, mode=mode, default=default)

        self.__stub = re.compile('^srm://[A-Za-z0-9:.\-/]+\?SFN=')

        # local imports are not available after the module hack at the end
        # of the file
        self.__sub = subprocess

    def execute(self, cmd, path, safe=False):
        args = ['lcg-' + cmd, '-b', '-D', 'srmv2', path]
        p = self.__sub.Popen(args, stdout=self.__sub.PIPE, stderr=self.__sub.PIPE)
        p.wait()
        if p.returncode != 0 and not safe:
            msg = "Failed to execute '{0}':\n{1}\n{2}".format(' '.join(args), p.stderr.read(), p.stdout.read())
            raise IOError(msg)
        return p.stdout.read()

    def strip(self, path):
        return self.__stub.sub('', path)

    def exists(self, path):
        try:
            self.execute('ls', path)
            return True
        except:
            return False

    def getsize(self, path):
        raise NotImplementedError

    def isdir(self, path):
        return True

    def isfile(self, path):
        return self.execute('ls', path, True).strip() == self.strip(path)

    def ls(self, path):
        pre = self.__stub.match(path).group(0)
        for p in self.execute('ls', path).splitlines():
            yield pre + p

    def makedirs(self, path):
        return True

    def remove(self, path):
        raise NotImplementedError
        self.execute('del', path)
