import logging
import os
import random
import re
import string
import subprocess
import xml.dom.minidom

from contextlib import contextmanager
from lobster.util import Configurable

import Chirp as chirp


logger = logging.getLogger('lobster.se')

# Breaks a URL down into 3 parts: the protocol, a optional server, and
# the path
url_re = re.compile(r'^([a-z]+)://([^/]*)(.*)/?$')


class FileSystem(object):

    """Singleton class as an interface for filesystem interactions.

    Needs to be configured before first use, with two lists of
    `StorageElement` implementations.  See the documentation of
    ``configure()`` for details.
    """

    _defaults = []
    _alternatives = []

    def __init__(self):
        self.__file__ = __file__
        self.__name__ = 'fs'

    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]

        def switch(*args, **kwargs):
            logger.debug(
                "resolving file system method '{0}' with arguments {1!r}, {2!r}".format(attr, args, kwargs))
            lasterror = None
            for imp in FileSystem._defaults:
                try:
                    return imp.fixresult(getattr(imp, attr)(*map(imp.lfn2pfn, args), **kwargs))
                except (IOError, OSError) as e:
                    logger.debug(
                        "method {0} of {1} failed with {2}, using args {3}, {4}".format(attr, imp, e, args, kwargs))
                    lasterror = e
                except TypeError as e:
                    logger.error("binding received an unexpected type; method {0} of {1} failed with {2}, using "
                                 "args {3}, {4}".format(attr, imp, e, args, kwargs))
                    lasterror = e
            raise AttributeError(
                "no resolution found for method '{0}' with arguments '{1}': {2}".format(attr, args, lasterror))
        return switch

    @classmethod
    def configure(cls, defaults, alternatives):
        """Configure the filesystem access methods.

        Parameters
        ----------
            defaults : list
                List of :class:`StorageElement` implementations.  These
                methods will be used in order by default to perform file
                system interactions.
            alternatives : list
                As `defaults`, specifies methods to perform file system
                interactions.  These methods are only active within the
                context of ``fs.alternative()``.
        """
        cls._defaults = defaults
        cls._alternatives = alternatives

    @contextmanager
    def alternative(self):
        tmp = FileSystem._defaults
        FileSystem._defaults = FileSystem._alternatives
        try:
            yield
        finally:
            FileSystem._defaults = tmp


class StorageElement(object):

    """Storage Element base class.

    Provides some basic handling of relative paths.  To be subclassed by
    implementations.
    """

    def __init__(self, pfnprefix):
        """Baseclass of a storage element.

        Parameters
        ----------
        pfnprefix : string
            The path prefix under which relative file names can be
            accessed.
        """
        self._pfnprefix = pfnprefix
        if not self._pfnprefix.endswith('/'):
            self._pfnprefix += '/'

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
        self.mkdir(path, mode=mode)


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
                raise IOError("path does not exist: {0}".format(path))
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

    def remove(self, *paths):
        for path in paths:
            try:
                os.remove(path)
            except OSError:
                pass


class Hadoop(StorageElement):

    def __init__(self, pfnprefix='/hadoop'):
        super(Hadoop, self).__init__(pfnprefix)

    def __execute(self, cmd, *paths, **kwargs):
        cmds = cmd.split()
        args = ['hadoop', 'fs', '-' + cmds[0]] + cmds[1:] + list(paths)
        try:
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            pout, perr = p.communicate()
            if p.returncode != 0 and not kwargs.get('safe', False):
                msg = "Failed to execute '{0}':\n{1}\n{2}".format(' '.join(args), perr, pout)
                raise IOError(msg)
        except OSError:
            raise AttributeError("hadoop filesystem utilities not available")
        return pout

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

    def remove(self, *paths):
        while len(paths) != 0:
            self.__execute('rm', *(paths[:50]))
            paths = paths[50:]


class Chirp(StorageElement):

    def __init__(self, server, pfnprefix):
        super(Chirp, self).__init__(pfnprefix)

        self.__c = chirp.Client(server, timeout=10)

    def exists(self, path):
        try:
            self.__c.stat(str(path))
            return True
        except IOError:
            return False

    def getsize(self, path):
        return self.__c.stat(str(path)).size

    def isdir(self, path):
        return len(self.__c.ls(str(path))) > 0

    def isfile(self, path):
        return len(self.__c.ls(str(path))) == 0

    def ls(self, path):
        for f in self.__c.ls(str(path)):
            if f.path not in ('.', '..'):
                yield os.path.join(path, f.path)

    def mkdir(self, path, mode=None):
        self.__c.mkdir(str(path))
        if mode:
            self.__c.chmod(str(path), mode)

    def permissions(self, path):
        return self.__c.stat(str(path)).mode & 0777

    def remove(self, *paths):
        for path in paths:
            self.__c.rm(str(path))


class SRM(StorageElement):

    def __init__(self, pfnprefix):
        super(SRM, self).__init__(pfnprefix)

    def execute(self, cmd, *paths, **kwargs):
        cmds = cmd.split()
        args = ['gfal-' + cmds[0]] + cmds[1:] + list(paths)
        try:
            p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env={})
            pout, err = p.communicate()
            if p.returncode != 0 and not kwargs.get('safe', False):
                msg = "Failed to execute '{0}':\n{1}\n{2}".format(' '.join(args), err, pout)
                raise IOError(msg)
        except OSError:
            raise AttributeError("srm utilities not available")
        return pout

    def exists(self, path):
        try:
            self.execute('stat', path)
            return True
        except Exception:
            return False

    def getsize(self, path):
        output = self.execute('stat', path)
        return output.splitlines()[1].split()[1]

    def isdir(self, path):
        try:
            output = self.execute('stat', path)
            return 'directory' in output.splitlines()[1]
        except Exception:
            return False

    def isfile(self, path):
        try:
            output = self.execute('stat', path)
            return 'regular file' in output.splitlines()[1]
        except Exception:
            return False

    def ls(self, path):
        for p in self.execute('ls', path).splitlines():
            yield os.path.join(path, p)

    def mkdir(self, path, mode=None):
        self.execute('mkdir -p', path)

    def permissions(self, path):
        output = self.execute('stat', path)
        try:
            return int(output.splitlines()[2][9:13], 8)
        except IndexError:
            raise IOError

    def remove(self, *paths):
        while len(paths) != 0:
            # FIXME safe is active because SRM does not care about directories.
            self.execute('rm -r', *(paths[:50]), safe=True)
            paths = paths[50:]


class StorageConfiguration(Configurable):

    """
    Container for storage element configuration.

    Uses URLs of the form `{protocol}://{server}/{path}` to specify input
    and output locations, where the `server` is omitted for `file` and
    `hadoop` access.  All output URLs should point to the same physical
    storage, to ensure consistent data handling.  Storage elements within
    CMS, as in `T2_CH_CERN` will be expanded for the `srm` and `root`
    protocol.  Protocols supported:

    * `file`
    * `gsiftp`
    * `hadoop`
    * `chirp`
    * `srm`
    * `root`

    The `chirp` protocol requires an instance of a `Chirp` server.

    Attributes modifiable at runtime:

    * `input`
    * `output`

    Parameters
    ----------
        output : list
            A list of URLs to access output storage
        input : list
            A list of URLs to access input data
        use_work_queue_for_inputs : bool
            Use `WorkQueue` to transfer input data.  Will encur a severe
            running penalty when used.
        use_work_queue_for_outputs : bool
            Use `WorkQueue` to transfer output data.  Will encur a severe
            running penalty when used.
        shuffle_inputs : bool
            Shuffle the list of input URLs when passing them to the task.
            Will provide basic load-distribution.
        shuffle_outputs : bool
            Shuffle the list of output URLs when passing them to the task.
            Will provide basic load-distribution.
        disable_input_streaming : bool
            Turn of streaming input data via, e.g., `XrootD`.
        disable_stage_in_acceleration : bool
            By default, tasks with many input files will test input URLs
            for the first successful one, which will then be used to access
            the remaining input files.  By using this setting, all input
            URLs will be attempted for all input files.
    """
    _mutable = {
        'input': (None, [], False),
        'output': (None, [], False)
    }

    # Map protocol shorthands to actual protocol names
    __protocols = {
        'gsiftp': 'gsiftp',
        'srm': 'srmv2',
        'root': 'xrootd'
    }

    # Matches CMS tiered computing site as found in
    # /cvmfs/cms.cern.ch/SITECONF/
    __site_re = re.compile(r'^T[0123]_(?:[A-Z]{2}_)?[A-Za-z0-9_\-]+$')

    def __init__(self,
                 output,
                 input=None,
                 use_work_queue_for_inputs=False,
                 use_work_queue_for_outputs=False,
                 shuffle_inputs=False,
                 shuffle_outputs=False,
                 disable_input_streaming=False,
                 disable_stage_in_acceleration=False):
        if input is None:
            self.input = []
        else:
            self.input = [
                self.expand_site(os.path.expanduser(os.path.expandvars(i))) for i in input]
        self.output = [self.expand_site(os.path.expanduser(os.path.expandvars(o))) for o in output]

        self.use_work_queue_for_inputs = use_work_queue_for_inputs
        self.use_work_queue_for_outputs = use_work_queue_for_outputs

        self.shuffle_inputs = shuffle_inputs
        self.shuffle_outputs = shuffle_outputs

        self.disable_input_streaming = disable_input_streaming
        self.disable_stage_in_acceleration = disable_stage_in_acceleration

        logger.debug("using input location {0}".format(self.input))
        logger.debug("using output location {0}".format(self.output))

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
            if 'destination-match' in e.attributes.keys() and \
                    not re.match(e.attributes['destination-match'].value, site):
                continue
            if path and len(path) > 0 and \
                    'path-match' in e.attributes.keys() and \
                    re.match(e.attributes['path-match'].value, path) is None:
                continue

            return e.attributes["path-match"].value, e.attributes["result"].value.replace('$1', r'\1')
        raise AttributeError(
            "No match found for protocol {0} at site {1}, using {2}".format(protocol, site, path))

    def expand_site(self, url):
        """Expands a CMS site label in a url to the corresponding server.

        >>> StorageConfiguration({}).expand_site('root://T3_US_NotreDame/store/user/spam/ham/eggs')
        u'root://xrootd.unl.edu//store/user/spam/ham/eggs'
        """
        protocol, server, path = url_re.match(url).groups()

        if self.__site_re.match(server) and protocol in self.__protocols:
            regexp, result = self._find_match(self.__protocols[protocol], server, path)
            return re.sub(regexp, result, path)

        if path.endswith('/'):
            path = path[:-1]

        return "{0}://{1}{2}/".format(protocol, server, path)

    def transfer_inputs(self):
        """Indicates whether input files need to be transferred manually.
        """
        return self.use_work_queue_for_inputs

    def transfer_outputs(self):
        """Indicates whether output files need to be transferred manually.
        """
        return self.use_work_queue_for_outputs

    def local(self, filename):
        for url in self.input + self.output:
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
                    yield Chirp(server, path)
                except chirp.AuthenticationFailure:
                    raise AttributeError("cannot access chirp server")
            elif protocol == 'file':
                yield Local(path)
            elif protocol == 'hdfs':
                try:
                    yield Hadoop(path)
                except NameError:
                    raise NotImplementedError("hadoop support is missing on this system")
            elif protocol == 'srm':
                SRM(url)
            else:
                logger.debug("implementation of master access missing for URL {0}".format(url))

    def deactivate(self):
        """Clear all storage-element objects.

        Needed when Lobster daemonizes, as filesystem objects may have
        connections open that will get closed by daemonizing.  Underlying
        objects may not realize that file descriptors are invalid and write
        to newly opened files.
        """
        FileSystem.configure([], [])

    def activate(self):
        """Sets file system access methods.

        Replaces default file system access methods with the ones specified
        per configuration for input and output storage element access.
        """
        FileSystem.configure(
            list(self._initialize(self.output)),
            list(self._initialize(self.input))
        )

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
        if self.shuffle_inputs:
            random.shuffle(self.input)
        if self.shuffle_outputs or (self.shuffle_inputs and merge):
            random.shuffle(self.output)

        parameters['input'] = self.input if not merge else self.output
        parameters['output'] = self.output
        parameters['disable streaming'] = self.disable_input_streaming
        if not self.disable_stage_in_acceleration:
            parameters['accelerate stage-in'] = 3
