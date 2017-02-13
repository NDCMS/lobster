import fnmatch
import glob
import hashlib
import logging
import re
import os
import shutil
import tarfile

import lobster.core
import lobster.util

__all__ = ['Sandbox']

logger = logging.getLogger('lobster.sandbox')
cache = {}


class Sandbox(lobster.core.Sandbox):

    """
    Packs a sandbox to run CMSSW out of.

    By default, all necessary directories, plus any `python` or `data`
    directories to be found under `src` are included.

    Parameters
    ----------
        include : list
            Directories or files to include, relative to the `src`
            directory of the release used.
        release : str
            The path to the CMSSW release to be used as a sandbox.
            Defaults to the environment variable `LOCALRT`.
    """

    _mutable = {}

    def __init__(self, include=None, release=None, blacklist=None, recycle=None):
        super(Sandbox, self).__init__(recycle, blacklist)
        if release:
            self.release = os.path.expandvars(os.path.expanduser(release))
        else:
            try:
                self.release = os.environ['LOCALRT']
            except KeyError:
                raise AttributeError("Need to be either in a `cmsenv` or specify a sandbox release!")
        self.include = include or []

    def __release2filename(self, indir, rel, arch):
        """Returns a filename for a given release top, i.e., the file system
        path of a release.
        """
        p = os.path.abspath(os.path.expandvars(os.path.expanduser(indir)))
        return "sandbox-{r}-{v}-{d}.tar.bz2".format(r=rel, v=arch, d=hashlib.sha1(p).hexdigest()[:7])

    def __dontpack(self, fn):
        res = ('/.' in fn and '/.SCRAM' not in fn) or '/CVS/' in fn
        if res:
            return True
        return False

    def _recycle(self, outdir):
        release_and_arch = re.compile(r'sandbox-(.*)-(slc.*)-[A-Fa-f0-9]*.tar.bz2$')
        shutil.copy2(self.recycle, outdir)
        m = release_and_arch.search(self.recycle)
        if not m:
            raise AttributeError("Can't determine CMSSW release and arch from recycled sandbox!")
        rtname, rtarch = m.groups()
        return rtname, rtarch, os.path.join(outdir, os.path.split(self.recycle)[-1])

    def _get_cmssw_arch(self, dirname):
        candidates = glob.glob('{}/.SCRAM/slc*'.format(dirname))
        if len(candidates) != 1:
            raise AttributeError("Can't determine SCRAM arch!")
        return os.path.basename(candidates[0])

    def _get_cmssw_version(self, dirname):
        with open(os.path.join(dirname, '.SCRAM', 'Environment')) as fd:
            for line in fd:
                k, v = line.strip().split('=')
                if k == 'SCRAM_PROJECTVERSION':
                    return v
            else:
                raise AttributeError("Can't determine CMSSW project version")

    def _package(self, basedirs, outdir):
        indir = lobster.util.findpath(basedirs, self.release)

        rtarch = self._get_cmssw_arch(indir)
        rtname = self._get_cmssw_version(indir)

        outfile = os.path.join(outdir, self.__release2filename(indir, rtname, rtarch))

        if os.path.exists(outfile):
            logger.info("reusing sandbox in {0}".format(outfile))
            return rtname, rtarch, outfile

        def ignore_file(fn):
            for test in self.blacklist:
                if fnmatch.fnmatch(os.path.split(fn)[1], test):
                    return True
            return False

        logger.info("packing sandbox into {0}".format(outfile))
        logger.debug("using release name {1} with base directory {0}".format(indir, rtname))
        tarball = tarfile.open(outfile, "w|bz2")

        # package bin, etc
        subdirs = ['bin', 'cfipython', 'external', 'lib', 'python']
        subdirs += [os.path.join('src', incl) for incl in self.include]

        for (path, dirs, files) in os.walk(os.path.join(indir, 'src')):
            for subdir in ['data', 'python', 'interface']:
                if subdir in dirs:
                    rtpath = os.path.join(os.path.relpath(path, indir), subdir)
                    subdirs.append(rtpath)

        for subdir in subdirs:
            if isinstance(subdir, tuple) or isinstance(subdir, list):
                (subdir, sandboxname) = subdir
            else:
                sandboxname = subdir
            inname = os.path.join(indir, subdir)
            if not os.path.exists(inname):
                continue

            outname = os.path.join(rtname, sandboxname)
            logger.debug("packing {0}".format(subdir))

            tarball.add(inname, outname, exclude=ignore_file)

        tarball.close()

        return rtname, rtarch, outfile
