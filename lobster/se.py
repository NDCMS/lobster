import glob
import multiprocessing
import os
import random
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
    """Container for storage element configuration.
    """

    # Map protocol shorthands to actual protocol names
    __protocols = {
            'srm': 'srmv2',
            'root': 'xrootd'
    }

    # Matches CMS tiered computing site as found in
    # /cvmfs/cms.cern.ch/SITECONF/
    __site_re = re.compile(r'^T[0123]_(?:[A-Z]{2}_)?[A-Za-z0-9_\-]+$')
    # Breaks a URL down into 3 parts: the protocol, a optional server, and
    # the path
    __url_re = re.compile(r'^([a-z]+)://([^/]*)(.*)/?$')

    def __init__(self, config):
        self.__input = map(self._expand_site, config.get('input', []))
        self.__output = map(self._expand_site, config.get('output', []))

        self.__wq_inputs = config.get('use work queue for inputs', False)
        self.__wq_outputs = config.get('use work queue for outputs', False)

        self.__shuffle_inputs = config.get('shuffle inputs', False)
        self.__shuffle_outputs = config.get('shuffle outputs', False)

        self.__no_streaming = config.get('disable input streaming', False)

        logger.debug("using input location {0}".format(self.__input))
        logger.debug("using output location {0}".format(self.__output))

    def _find_match(self, protocol, site, path):
        """Extracts the LFN to PFN translation from the SITECONF.

        >>> StorageConfiguration({})._find_match('xrootd', 'T3_US_NotreDame', '/store/user/spam/ham/eggs')
        (u'/+store/(.*)', u'root://xrootd.unl.edu//store/\\\\1')
        """
        file = os.path.join('/cvmfs/cms.cern.ch/SITECONF', site, 'PhEDEx/storage.xml')
        doc = xml.dom.minidom.parse(file)

        for e in doc.getElementsByTagName("lfn-to-pfn"):
            if e.attributes["protocol"].value != protocol:
                continue
            if e.attributes.has_key('destination-match') and \
                    not re.match(e.attributes['destination-match'].value, site):
                continue
            if path and len(path) > 0 and \
                    e.attributes.has_key('path-match') and \
                    re.match(e.attributes['path-match'].value, path) is None:
                continue

            return e.attributes["path-match"].value, e.attributes["result"].value.replace('$1', r'\1')

    def _expand_site(self, url):
        """Expands a CMS site label in a url to the corresponding server.

        >>> StorageConfiguration({})._expand_site('root://T3_US_NotreDame/store/user/spam/ham/eggs')
        u'root://xrootd.unl.edu//store/user/spam/ham/eggs'
        """
        protocol, server, path = self.__url_re.match(url).groups()

        if self.__site_re.match(server) and protocol in self.__protocols:
            regexp, result = self._find_match(self.__protocols[protocol], server, path)
            return re.sub(regexp, result, path)

        return "{0}://{1}{2}/".format(protocol, server, path)

    def transfer_inputs(self):
        """Indicates whether input files need to be transferred manually.
        """
        return self.__wq_inputs

    def transfer_outputs(self):
        """Indicates whether output files need to be transferred manually.
        """
        return self.__wq_outputs

    def pfn(self, path):
        if self.__local:
            return path.replace(self.__base, self.__local)
        raise IOError("Can't create LFN without local storage access")

    def activate(self):
        """Replaces default file system access methods with the ones
        specified per configuration for output storage element access.
        """
        StorageElement.store()
        StorageElement.reset()

        for url in self.__output:
            protocol, server, path = self.__url_re.match(url).groups()

            if protocol == 'chirp':
                Chirp(server, path, '')
            elif protocol == 'file':
                Local(lfn2pfn=('', path), pfn2lfn=(path, ''))
            elif protocol == 'hdfs':
                try:
                    Hadoop(lfn2pfn=('', path), pfn2lfn=(path, ''))
                except NameError:
                    raise NotImplementedError("hadoop support is missing on this system")
            elif protocol == 'srm':
                Local(lfn2pfn=('', path), pfn2lfn=(path, ''))
            else:
                logger.debug("implementation of master access missing for URL {0}".format(url))

    def preprocess(self, parameters, merge):
        """Adjust the storage transfer parameters sent with a task.

        Parameters
        ----------
        parameters : dict
            The task parameters to alter.  This method will add keys
            'input', 'output', and 'disable streaming'.
        merge : bool
            Specify if this is a merging parameter set.
        """
        if self.__shuffle_inputs:
            random.shuffle(self.__input)
        if self.__shuffle_outputs or (self.__shuffle_inputs and merge):
            random.shuffle(self.__output)

        parameters['input'] = self.__input if not merge else self.__output
        parameters['output'] = self.__output
        parameters['disable streaming'] = self.__no_streaming
