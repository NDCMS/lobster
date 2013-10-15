from itertools import imap
import os
import re
import sys
import tarfile

class Test(object):
    def __init__(self, match):
        if match.startswith('/') and match.endswith('/'):
            self._test = re.compile(match[1:-1]).search
        else:
            match = '/{0}/'.format(match)
            self._test = lambda s: match in s

    def __call__(self, fn):
        return self._test(fn)

def package(indir, outfile, excludes=['.git', 'CVS', '.SCRAM', 'test']):
    tests = map(Test, excludes)

    tarball = tarfile.open(outfile, 'w:bz2')
    try:
        def exclusion(fn):
            for t in tests:
                if t(fn):
                    return True
            print fn
            # sys.stdout.write("\r\kadding {0}".format(fn))
            return False

        tarname = os.path.split(os.path.normpath(indir))[1]
        tarball.add(indir, tarname, exclude=exclusion)
    finally:
        tarball.close()
