import glob
import multiprocessing
import os
import re
import subprocess
import xml.dom.minidom

from contextlib import contextmanager
from functools import partial, wraps

logger = multiprocessing.get_logger()

class StorageElement(object):
    _defaults = []
    _systems = []

    def __init__(self, lfn2pfn=None, pfn2lfn=None):
        """Create or use a StorageElement abstraction.

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

        def switch(path=None):
            for imp in self._systems:
                if imp.matches(path):
                    return imp.fixresult(getattr(imp, attr)(imp.lfn2pfn(path)))
            raise AttributeError("no path resolution found for '{0}'".format(path))
        return switch

    def matches(self, path):
        return re.match(self._lfn2pfn[0], path) is not None

    def lfn2pfn(self, path):
        return re.sub(*(list(self._lfn2pfn) + [path, 1]))

    def fixresult(self, res):
        def pfn2lfn(p):
            return re.sub(*(list(self._pfn2lfn) + [p, 1]))

        if isinstance(res, str):
            return pfn2lfn(res)

        try:
            return map(pfn2lfn, res)
        except TypeError:
            return res

    @classmethod
    def reset(cls):
        cls._systems = []

    @classmethod
    def test(cls, path):
        for system in cls._systems:
            try:
                list(system.ls(system.lfn2pfn(path)))
            except:
                list(system.ls(path))

    @classmethod
    def store(cls):
        cls._defaults = list(cls._systems)

    @contextmanager
    def default(self):
        tmp = self._systems
        self._systems = self._defaults
        try:
            yield
        finally:
            self._systems = tmp

class Local(StorageElement):
    def __init__(self, lfn2pfn=('(.*)', r'\1'), pfn2lfn=('(.*)', r'\1')):
        super(Local, self).__init__(lfn2pfn, pfn2lfn)
        self.exists = os.path.exists
        self.getsize = os.path.getsize
        self.isdir = os.path.isdir
        self.isfile = os.path.isfile
        self.ls = glob.glob
        self.makedirs = os.makedirs
        self.remove = os.remove

try:
    import hadoopy

    class Hadoop(StorageElement):
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
except:
    pass

class Chirp(StorageElement):
    def __init__(self, server, chirppath, basepath):
        super(Chirp, self).__init__((basepath, chirppath), (chirppath, basepath))

        self.__server = server
        self.__sub = subprocess

    def execute(self, *args, **kwargs):
        cmd = ["chirp", "-t", "10", self.__server] + list(args)
        p = self.__sub.Popen(
                cmd,
                stdout=self.__sub.PIPE,
                stderr=self.__sub.PIPE,
                stdin=self.__sub.PIPE)
        p.wait()

        if p.returncode != 0 and not kwargs.get("safe", False):
            raise subprocess.CalledProcessError(p.returncode,
                    " ".join(["chirp", self.__server] + list(args)))

        return p.stdout.read()

    def exists(self, path):
        out = self.execute("stat", path)
        return len(out.splitlines()) > 1

    def getsize(self, path):
        raise NotImplementedError

    def isdir(self, path):
        out = self.execute('stat', path)
        return out.splitlines()[4].split() == ["nlink:", "0"]

    def isfile(self, path):
        out = self.execute('stat', path)
        return out.splitlines()[4].split() != ["nlink:", "0"]

    def ls(self, path):
        out = self.execute('ls', '-la', path)
        for l in out.splitlines():
            if l.startswith('d'):
                continue
            yield os.path.join(path, l.split(None, 9)[8])

    def makedirs(self, path):
        self.execute('mkdir', '-p', path)

    def remove(self, path):
        # FIXME remove does not work for directories
        self.execute('rm', path)

class SRM(StorageElement):
    def __init__(self, lfn2pfn=('srm://', 'srm://'), pfn2lfn=('srm://', 'srm://')):
        super(SRM, self).__init__(lfn2pfn, pfn2lfn)

        self.__stub = re.compile('^srm://[A-Za-z0-9:.\-/]+\?SFN=')

        # local imports are not available after the module hack at the end
        # of the file
        self.__sub = subprocess

    def execute(self, cmd, path, safe=False):
        args = ['lcg-' + cmd, '-b', '-D', 'srmv2', path]
        try:
            p = self.__sub.Popen(args, stdout=self.__sub.PIPE, stderr=self.__sub.PIPE)
            p.wait()
            if p.returncode != 0 and not safe:
                msg = "Failed to execute '{0}':\n{1}\n{2}".format(' '.join(args), p.stderr.read(), p.stdout.read())
                raise IOError(msg)
        except OSError:
            raise AttributeError("srm utilities not available")
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
        # FIXME safe is active because SRM does not care about directories.
        self.execute('del', path, safe=True)

class StorageConfiguration(object):
    def __init__(self, config):
        self.__base = config.get('base', '').rstrip('/')
        self.__input = None
        self.__output = None
        self.__local = None

        self._discover(config.get('site'))

        if 'input' in config:
            self.__input = config['input'].rstrip('/')
        if 'output' in config:
            self.__output = config['output'].rstrip('/')

        self.__hadoop = config.get('hadoop')
        if 'local' in config:
            self.__local = config['local'].rstrip('/')

        logger.debug("using input location {0}".format(self.__input))
        logger.debug("using output location {0}".format(self.__output))

    def _find_match(self, doc, tag, protocol):
        for e in doc.getElementsByTagName(tag):
            if e.attributes["protocol"].value != protocol:
                continue
            if e.attributes.has_key('destination-match') and \
                    not re.match(e.attributes['destination-match'].value, self.__site):
                continue
            if self.__base and len(self.__base) > 0 and \
                    e.attributes.has_key('path-match') and \
                    re.match(e.attributes['path-match'].value, self.__base) is None:
                continue

            return e.attributes["path-match"].value, e.attributes["result"].value.replace('$1', r'\1')

    def _discover(self, site):
        if not site:
            return

        if len(self.__base) == 0:
            raise KeyError("can't run CMS site discovery without 'base' path.")

        self.__site = site

        file = os.path.join('/cvmfs/cms.cern.ch/SITECONF', site, 'PhEDEx/storage.xml')
        doc = xml.dom.minidom.parse(file)

        regexp, result = self._find_match(doc, "lfn-to-pfn", "xrootd")
        self.__input = re.sub(regexp, result, self.__base)
        regexp, result = self._find_match(doc, "lfn-to-pfn", "srmv2")
        self.__output = re.sub(regexp, result, self.__base)

    @property
    def path(self):
        return self.__base

    def transfer_inputs(self):
        """Indicates whether input files need to be transferred manually.
        """
        return self.__input is None

    def transfer_outputs(self):
        """Indicates whether output files need to be transferred manually.
        """
        return self.__output is None

    def pfn(self, path):
        if self.__local:
            return path.replace(self.__base, self.__local)
        raise IOError("Can't create LFN without local storage access")

    def activate(self):
        """Replaces default file system access methods with the ones
        specified per configuration.
        """
        StorageElement.store()
        StorageElement.reset()

        if self.__hadoop:
            try:
                Hadoop(lfn2pfn=(self.__base, self.__hadoop), pfn2lfn=(self.__hadoop, self.__base))
            except NameError:
                raise NotImplementedError("hadoop support is missing on this system")
        if self.__local:
            Local(lfn2pfn=(self.__base, self.__local), pfn2lfn=(self.__local, self.__base))
        if self.__output:
            if self.__output.startswith("srm://"):
                SRM(lfn2pfn=(self.__base, self.__output), pfn2lfn=(self.__output, self.__base))
            elif self.__output.startswith("chirp://"):
                server, path = re.match("chirp://([a-zA-Z0-9:.\-]+)/(.*)", self.__output).groups()
                Chirp(server, path, self.__base)

        StorageElement.test(self.__output.replace(self.__base, '', 1))

    def preprocess(self, parameters, localdata):
        """Adjust the input, output files within the parameters send with a task.

        Parameters
        ----------
        parameters : dict
            The task parameters to alter.  This should contain the keys
            'output files' and 'mask', the latter with a subkey 'files'.
        localdata : bool
            Indicates whether input file translation is needed or not.
        """
        if self.__output:
            outputs = parameters['output files']
            parameters['output files'] = [(src, tgt.replace(self.__base, self.__output)) for src, tgt in outputs]
        if self.__input and localdata:
            inputs = parameters['mask']['files']
            parameters['mask']['files'] = [f.replace(self.__base, self.__input) for f in inputs]

    def postprocess(self, report):
        if self.__input:
            infos = report['files']['info']
            report['files']['info'] = dict((k.replace(self.__input, self.__base), v) for k, v in infos.items())
            skipped = report['files']['skipped']
            report['files']['skipped'] = [f.replace(self.__input, self.__base) for f in skipped]
