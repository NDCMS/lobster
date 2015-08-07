import glob
import multiprocessing
import os
import random
import re
import string
import subprocess
import xml.dom.minidom

from contextlib import contextmanager
from functools import partial, wraps

import Chirp as chirp

logger = multiprocessing.get_logger()

# Breaks a URL down into 3 parts: the protocol, a optional server, and
# the path
url_re = re.compile(r'^([a-z]+)://([^/]*)(.*)/?$')

class StorageElement(object):
    """Weird class to handle all needs of storage implementations.

    This class can be used for file system operations after at least one of
    its subclasses has been instantiated.

    "One size fits nobody." (T. Pratchett)
    """
    _defaults = []
    _systems = []

    def __init__(self, pfnprefix=None):
        """Create or use a StorageElement abstraction.

        As a user, use with no parameters to access various storage
        elements transparently.

        Subclasses should call the constructor with appropriate arguments,
        which should also be made available to the user.

        Parameters
        ----------
        pfnprefix : string, optional
            The path prefix under which relative file names can be
            accessed.
        """
        self.__master = False

        if pfnprefix is not None:
            self._pfnprefix = pfnprefix
            if not self._pfnprefix.endswith('/'):
                self._pfnprefix += '/'
            StorageElement._systems.append(self)
        else:
            self.__master = True

    def __getattr__(self, attr):
        if attr in self.__dict__ or not self.__master:
            return self.__dict__[attr]

        def switch(path, *args):
            lasterror = None
            for imp in StorageElement._systems:
                try:
                    return imp.fixresult(getattr(imp, attr)(imp.lfn2pfn(path), *args))
                except (IOError, OSError) as e:
                    lasterror = e
            raise AttributeError("no resolution found for path '{0}' and method {1}: {2}".format(path, attr, lasterror))
        return switch

    def lfn2pfn(self, path):
        if path.startswith('/'):
            p = os.path.join(self._pfnprefix, path[1:])
        else:
            p = os.path.join(self._pfnprefix, path)
        m = url_re.match(p)
        if m:
            protocol, server, path = url_re.match(p).groups()
            path = os.path.normpath(path)
            return "{0}://{1}{2}/".format(protocol, server, path)
        return os.path.normpath(p)

    def fixresult(self, res):
        def pfn2lfn(p):
            return p.replace(self._pfnprefix, '', 1)

        if isinstance(res, str):
            return pfn2lfn(res)

        try:
            return map(pfn2lfn, res)
        except TypeError:
            return res

    def makedirs(self, path):
        if re.match(r'^..(?:/..)*$', path):
            if len(path) > 2 + 100 * 3:
                # fail for excessive path recursion
                raise NotImplementedError
            parent = os.path.join(path, '..')
        elif path == '':
            parent = '..'
        else:
            parent = os.path.dirname(path)
        if not self.exists(parent):
            self.makedirs(parent)
        if self.exists(path):
            return
        mode = self.permissions(parent)
        self.mkdir(path, mode)

    @classmethod
    def reset(cls):
        cls._systems = []

    @classmethod
    def store(cls):
        cls._defaults = list(cls._systems)

    @contextmanager
    def default(self):
        tmp = StorageElement._systems
        StorageElement._systems = self._defaults
        try:
            yield
        finally:
            StorageElement._systems = tmp

class Local(StorageElement):
    def __init__(self, pfnprefix=''):
        super(Local, self).__init__(pfnprefix)
        self.exists = os.path.exists
        self.getsize = os.path.getsize
        self.isdir = self._guard(os.path.isdir)
        self.isfile = self._guard(os.path.isfile)

    def _guard(self, method):
        """Protect method against non-existent paths.
        """
        def guarded(path):
            if not os.path.exists(path):
                raise IOError()
            return method(path)
        return guarded

    def ls(self, path):
        for fn in os.listdir(path):
            yield os.path.join(path, fn)

    def mkdir(self, path, mode=None):
        os.mkdir(path)
        if mode:
            os.chmod(path, mode)

    def permissions(self, path):
        return os.stat(path).st_mode & 0777

    def remove(self, path):
        try:
            os.remove(path)
        except OSError:
            pass

class Hadoop(StorageElement):
    def __init__(self, pfnprefix='/hadoop'):
        super(Hadoop, self).__init__(pfnprefix)

    def __execute(self, cmd, path, safe=False):
        cmds = cmd.split()
        args = ['hadoop', 'fs', '-' + cmds[0]] + cmds[1:] + [path]
        try:
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            p.wait()
            if p.returncode != 0 and not safe:
                msg = "Failed to execute '{0}':\n{1}\n{2}".format(' '.join(args), p.stderr.read(), p.stdout.read())
                raise IOError(msg)
        except OSError:
            raise AttributeError("hadoop filesystem utilities not available")
        return p.stdout.read()

    def exists(self, path):
        try:
            self.__execute('stat', path)
            return True
        except IOError:
            return False

    def getsize(self, path):
        return int(self.__execute('stat %b', path))

    def isdir(self, path):
        return self.__execute('stat %F', path).strip() == 'directory'

    def isfile(self, path):
        return self.__execute('stat %F', path).strip() == 'regular file'

    def ls(self, path):
        for line in self.__execute('ls', path).splitlines()[1:]:
            yield line.split(None, 7)[-1]

    def mkdir(self, path, mode):
        self.__execute('mkdir', path)
        self.__execute('chmod ' + oct(mode), path)

    def permissions(self, path):
        output = self.__execute('ls -d', path).splitlines()[1].split(None, 1)[0]
        tr = string.maketrans('rwx-', '1110')
        # first character of output is either 'd' or '-' and needs to be
        # removed
        return int(output[1:].translate(tr), 2)

    def remove(self, path):
        self.__execute('rm', path)

class Chirp(StorageElement):
    def __init__(self, server, pfnprefix):
        super(Chirp, self).__init__(pfnprefix)

        self.__c = chirp.Client(server, timeout=10)
        self.remove = self.__c.rm

    def exists(self, path):
        try:
            self.__c.stat(path)
            return True
        except IOError:
            return False

    def getsize(self, path):
        return self.__c.stat(path).size

    def isdir(self, path):
        return len(self.__c.ls(path)) > 0

    def isfile(self, path):
        return len(self.__c.ls(path)) == 0

    def ls(self, path):
        for f in self.__c.ls(path):
            if f.path not in ('.', '..'):
                yield os.path.join(path, f.path)

    def mkdir(self, path, mode=None):
        self.__c.mkdir(path)
        if mode:
            self.__c.chmod(path, mode)

    def permissions(self, path):
        return self.__c.stat(path).mode & 0777

class SRM(StorageElement):
    def __init__(self, pfnprefix):
        super(SRM, self).__init__(pfnprefix)

    def execute(self, cmd, path, safe=False):
        cmds = cmd.split()
        args = ['gfal-' + cmds[0]] + cmds[1:] + [path]
        try:
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={})
            p.wait()
            if p.returncode != 0 and not safe:
                msg = "Failed to execute '{0}':\n{1}\n{2}".format(' '.join(args), p.stderr.read(), p.stdout.read())
                raise IOError(msg)
        except OSError:
            raise AttributeError("srm utilities not available")
        return p.stdout.read()

    def exists(self, path):
        try:
            self.execute('stat', path)
            return True
        except:
            return False

    def getsize(self, path):
        output = self.execute('stat', path, True)
        return output.splitlines()[1].split()[1]

    def isdir(self, path):
        try:
            output = self.execute('stat', path)
            return 'directory' in output.splitlines()[1]
        except:
            return False

    def isfile(self, path):
        try:
            output = self.execute('stat', path)
            return 'regular file' in output.splitlines()[1]
        except:
            return False

    def ls(self, path):
        for p in self.execute('ls', path).splitlines():
            yield os.path.join(path, p)

    def mkdir(self, path, mode=None):
        self.execute('mkdir -p', path)

    def permissions(self, path):
        output = self.execute('stat', path, True)
        try:
            return int(output.splitlines()[2][9:13], 8)
        except IndexError:
            raise IOError

    def remove(self, path):
        # FIXME safe is active because SRM does not care about directories.
        self.execute('rm -r', path, safe=True)

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
        raise AttributeError("No match found for protocol {0} at site {1}, using {2}".format(protocol, site, path))

    def _expand_site(self, url):
        """Expands a CMS site label in a url to the corresponding server.

        >>> StorageConfiguration({})._expand_site('root://T3_US_NotreDame/store/user/spam/ham/eggs')
        u'root://xrootd.unl.edu//store/user/spam/ham/eggs'
        """
        protocol, server, path = url_re.match(url).groups()

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

    def local(self, filename):
        for url in self.__input + self.__output:
            protocol, server, path = url_re.match(url).groups()

            if protocol != 'file':
                continue

            fn = os.path.join(path, filename)
            if os.path.isfile(fn):
                return fn
        raise IOError("Can't create LFN without local storage access")

    def _initialize(self, methods):
        for url in methods:
            protocol, server, path = url_re.match(url).groups()

            if protocol == 'chirp':
                try:
                    Chirp(server, path)
                except chirp.AuthenticationFailure:
                    raise AttributeError("cannot access chirp server")
            elif protocol == 'file':
                Local(path)
            elif protocol == 'hdfs':
                try:
                    Hadoop(path)
                except NameError:
                    raise NotImplementedError("hadoop support is missing on this system")
            elif protocol == 'srm':
                SRM(url)
            else:
                logger.debug("implementation of master access missing for URL {0}".format(url))

    def activate(self):
        """Sets file system access methods.

        Replaces default file system access methods with the ones specified
        per configuration for input and output storage element access.
        """
        StorageElement.reset()

        self._initialize(self.__input)

        StorageElement.store()
        StorageElement.reset()

        self._initialize(self.__output)

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
