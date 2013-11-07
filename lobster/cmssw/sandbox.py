from itertools import imap
import os
import re
import sys
# import tarfile

def dontpack(fn):
    res = ('/.' in fn and not '/.SCRAM' in fn) or '/CVS/' in fn
    if res:
        return True
    return False

def package(indir, outfile):
    try:
        # tarball = tarfile.open(outfile, 'w:bz2')
        # tarball.dereference = True

        rtname = os.path.split(os.path.normpath(indir))[1]

        # package bin, etc
        subdirs = ['.SCRAM', 'bin', 'config', 'lib', 'module', 'python']

        subdirs.append((os.environ['X509_USER_PROXY'], 'proxy'))

        for (path, dirs, files) in os.walk(indir):
            if 'data' not in dirs:
                continue

            rtpath = os.path.join(os.path.relpath(path, indir), 'data')
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
            print "packing", subdir
            # tarball.add(inname, outname, exclude=dontpack)

        # tarball.close()
    except:
        raise
