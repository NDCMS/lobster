from itertools import imap
import os
import re
import sys
import tarfile

def dontpack(fn):
    return '/.' in fn or '/CVS/' in fn

def package(indir, outfile):
    try:
        tarball = tarfile.open(outfile, 'w:bz2')
        tarball.dereference = True

        rtname = os.path.split(os.path.normpath(indir))[1]

        # package bin, etc
        subdirs = ['bin', 'lib', 'module', 'python']

        for (path, dirs, files) in os.walk(indir):
            if 'data' not in dirs:
                continue

            rtpath = os.path.join(os.path.relpath(path, indir), 'data')
            subdirs.append(rtpath)

        for subdir in subdirs:
            inname = os.path.join(indir, subdir)
            if not os.path.isdir(inname):
                continue
            outname = os.path.join(rtname, subdir)
            print "packing", subdir
            tarball.add(inname, outname, exclude=dontpack)

        tarball.close()
    except:
        raise
