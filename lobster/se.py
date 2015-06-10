import glob
import hadoopy
import os
import re
import subprocess

from functools import partial

class FileSystem(object):
    _systems = []

    def __init__(self, lfn2pfn=None, pfn2lfn=None):
        """Create or use a FileSystem abstraction.

        As a user, use with no parameters to access various storage
        elements transparently.

        Subclasses should call the constructor with appropriate arguments,
        which should also be made available to the user.

        Parameters
        ----------
        lfn2pfn : tuple, optional
            Contains a tuple out of a regexp and a replacement to perform
            path transformations from LFN to PFN.
        pfn2lfn : tuple, optional
            Contains a tuple out of a regexp and a replacement to perform
            path transformations from PFN to LFN.
        """
        self.__master = False

        if lfn2pfn is not None:
            self._lfn2pfn = lfn2pfn
            self._pfn2lfn = pfn2lfn
            self._systems.append(self)
        else:
            self.__master = True

    def __getattr__(self, attr):
        if attr in self.__dict__ or not self.__master:
            return self.__dict__[attr]

        def switch(path):
            for imp in self._systems:
                if imp.matches(path):
                    return imp.fixresult(getattr(imp, attr)(imp.fixpath(path)))
        return switch

    def matches(self, path):
        return re.match(self._lfn2pfn[0], path) is not None

    def fixpath(self, path):
        return re.sub(*(list(self._lfn2pfn) + [path]))

    def fixresult(self, res):
        def fixup(p):
            return re.sub(*(list(self._pfn2lfn) + [p]))

        if isinstance(res, str):
            return fixup(res)

        try:
            return map(fixup, res)
        except TypeError:
            return res

    @classmethod
    def reset(cls):
        cls._systems = []

class Local(FileSystem):
    def __init__(self, lfn2pfn=('(.*)', r'\1'), pfn2lfn=('(.*)', r'\1')):
        super(Local, self).__init__(lfn2pfn, pfn2lfn)
        self.exists = os.path.exists
        self.getsize = os.path.getsize
        self.isdir = os.path.isdir
        self.isfile = os.path.isfile
        self.ls = glob.glob
        self.makedirs = os.makedirs
        self.remove = os.remove

class Hadoop(FileSystem):
    def __init__(self, lfn2pfn=('/hadoop(/.*)', r'\1'), pfn2lfn=('/(.*)', r'/hadoop/\1')):
        super(Hadoop, self).__init__(lfn2pfn, pfn2lfn)

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
    def __init__(self, lfn2pfn=('srm://', 'srm://'), pfn2lfn=('srm://', 'srm://')):
        super(SRM, self).__init__(lfn2pfn, pfn2lfn)

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
