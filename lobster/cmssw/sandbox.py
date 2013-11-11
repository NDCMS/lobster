from itertools import imap
import os
import re
import shutil
import sys

def dontpack(fn):
    res = ('/.' in fn and not '/.SCRAM' in fn) or '/CVS/' in fn
    if res:
        return True
    return False

def package(indir, outdir):
    try:
        print "Creating sandbox in", outdir
        rtname = os.path.split(os.path.normpath(indir))[1]

        shutil.copy2(os.environ['X509_USER_PROXY'], os.path.join(outdir, 'proxy'))

        # package bin, etc
        subdirs = ['.SCRAM', 'bin', 'config', 'lib', 'module', 'python']

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
            outname = os.path.join(outdir, rtname, sandboxname)
            sys.stdout.write("packing {0}\x1b[K\r".format(subdir))
            sys.stdout.flush()
            if os.path.isdir(inname):
                shutil.copytree(inname, outname)
            else:
                shutil.copy2(inname, outname)
    except:
        raise

    sys.stdout.write("\r\x1b[K")
    sys.stdout.flush()
