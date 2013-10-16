#!/usr/bin/env python

import os
import tarfile
import subprocess
import sys

class Shell(object):
    def __init__(self, log, environ=None):
        outlog = log + '.out'
        errlog = log + '.err'
        self._cmdformat = "%%s 1>> '%s' 2>> '%s'; echo $?\n" % (outlog, errlog)
        self._shell = subprocess.Popen("bash", stdout=subprocess.PIPE,
                stderr=subprocess.PIPE, stdin=subprocess.PIPE, env=environ)

    def __lshift__(self, cmd):
        self._shell.stdin.write(self._cmdformat % (cmd,))
        retval = int(self._shell.stdout.readline())

        if retval != 0:
            raise SystemError, retval

(outboxfile, sandboxfile, configfile) = sys.argv[1:4]
args = sys.argv[4:]

sandbox = tarfile.open(sandboxfile, 'r:bz2')
sandbox.extractall()

for d in os.listdir('.'):
    if d.startswith('CMSSW'):
        break

env = os.environ
env['X509_USER_PROXY'] = os.path.join(d, 'proxy')

sh = Shell('cmssw', env)
exit_code = 0

try:
    sh << 'voms-proxy-info'
    sh << 'cd "%s"' % (d,)
    sh << 'eval $(scramv1 runtime -sh)'
    sh << 'cd -'
    sh << 'cmsRun -j report.xml "%s" %s' % (configfile, ' '.join(args))
except:
    exit_code = 123

outbox = tarfile.open(outboxfile, 'w:bz2')
try:
    outbox.add('report.xml')
    outbox.add('cmssw.out')
    outbox.add('cmssw.err')
except:
    pass
outbox.close()

sys.exit(exit_code)
