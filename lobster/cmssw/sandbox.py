import fnmatch
import hashlib
import logging
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

    def __release2filename(self, rel):
        """Returns a filename for a given release top, i.e., the file system
        path of a release.
        """
        p = os.path.abspath(os.path.expandvars(os.path.expanduser(rel)))
        v = os.path.split(p)[1]
        return "sandbox-{v}-{d}.tar.bz2".format(v=v, d=hashlib.sha1(p).hexdigest()[:7])

    def __dontpack(self, fn):
        res = ('/.' in fn and '/.SCRAM' not in fn) or '/CVS/' in fn
        if res:
            return True
        return False

    def _recycle(self, outdir):
        shutil.copy2(self._recycle, outdir)
        for file in tarfile.open(self._recycle):
            if ".SCRAM" in file.name:
                rtname = os.path.dirname(os.path.normpath(file.name)).split("/")[0]
                break
        return rtname, os.path.join(outdir, os.path.split(self._recycle)[-1])

    def _package(self, basedirs, outdir):
        indir = lobster.util.findpath(basedirs, self.release)
        rtname = os.path.split(os.path.normpath(indir))[1]
        outfile = os.path.join(outdir, self.__release2filename(indir))

        if os.path.exists(outfile):
            logger.info("reusing sandbox in {0}".format(outfile))
            return rtname, outfile

        def ignore_file(fn):
            for test in self.blacklist:
                if fnmatch.fnmatch(os.path.split(fn)[1], test):
                    return True
            return False

        logger.info("packing sandbox into {0}".format(outfile))
        logger.debug("using release name {1} with base directory {0}".format(indir, rtname))
        tarball = tarfile.open(outfile, "w|bz2")

        # package bin, etc
        subdirs = ['.SCRAM', 'bin', 'cfipython', 'config', 'lib', 'module', 'python']
        subdirs += [os.path.join('src', incl) for incl in self.include]

        for (path, dirs, files) in os.walk(os.path.join(indir, 'src')):
            for subdir in ['data', 'python']:
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

        return rtname, outfile
