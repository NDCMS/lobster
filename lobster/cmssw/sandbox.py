import logging
import os
import re
import shutil
import tarfile

def dontpack(fn):
    res = ('/.' in fn and not '/.SCRAM' in fn) or '/CVS/' in fn
    if res:
        return True
    return False

def package(indir, outdir, blacklist=[], recycle=None):
    logging.info("creating sandbox in {0}".format(outdir))

    if not os.path.exists(outdir):
        os.makedirs(outdir)

    rtname = os.path.split(os.path.normpath(indir))[1]

    if recycle:
        shutil.copy2(recycle, os.path.split(outdir)[0])

        tarball = tarfile.open(recycle)
        tarball.extractall(outdir)
        tarball.close()
    else:
        # package bin, etc
        subdirs = ['.SCRAM', 'bin', 'config', 'lib', 'module', 'python']

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
            outname = os.path.join(outdir, rtname, sandboxname)
            logging.debug("packing {0}".format(subdir))
            if os.path.isdir(inname):
                shutil.copytree(inname, outname, symlinks=True, ignore=shutil.ignore_patterns(*blacklist))
            else:
                shutil.copy2(inname, outname)

        outfile = (outdir if not outdir.endswith("/") else outdir[:-1]) + ".tar.bz2"
        logging.info("packing sandbox into {0}".format(outfile))
        tarball = tarfile.open(outfile, "w|bz2")
        for entry in os.listdir(outdir):
            tarball.add(os.path.join(outdir, entry), entry)
        tarball.close()
