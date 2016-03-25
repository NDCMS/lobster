import fnmatch
import hashlib
import logging
import os
import shutil
import tarfile

__all__ = ['package']

logger = logging.getLogger('lobster.sandbox')
cache = {}

def release2filename(rel):
    """Returns a filename for a given release top, i.e., the file system
    path of a release.
    """
    p = os.path.abspath(os.path.expandvars(os.path.expanduser(rel)))
    v = os.path.split(p)[1]
    return "sandbox-{v}-{d}.tar.bz2".format(v=v, d=hashlib.sha1(p).hexdigest()[:7])

def dontpack(fn):
    res = ('/.' in fn and not '/.SCRAM' in fn) or '/CVS/' in fn
    if res:
        return True
    return False

def recycle(fn, outdir):
    shutil.copy2(fn, outdir)
    for file in tarfile.open(fn):
        if ".SCRAM" in file.name:
            rtname = os.path.dirname(os.path.normpath(file.name)).split("/")[0]
            break
    return rtname, os.path.join(outdir, os.path.split(fn)[-1])

def package(indir, outdir, blacklist=None):
    if blacklist is None:
        blacklist = []

    rtname = os.path.split(os.path.normpath(indir))[1]
    outfile = os.path.join(outdir, release2filename(indir))

    if os.path.exists(outfile):
        logger.info("reusing sandbox in {0}".format(outfile))
        return rtname, outfile

    def ignore_file(fn):
        for test in blacklist:
            if fnmatch.fnmatch(os.path.split(fn)[1], test):
                return True
        return False

    logger.info("packing sandbox into {0}".format(outfile))
    logger.debug("using release name {1} with base directory {0}".format(indir, rtname))
    tarball = tarfile.open(outfile, "w|bz2")

    # package bin, etc
    subdirs = ['.SCRAM', 'bin', 'cfipython', 'config', 'lib', 'module', 'python']

    for (path, dirs, files) in os.walk(os.path.join(indir, 'src')):
        if any(d in blacklist for d in dirs):
            continue

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
