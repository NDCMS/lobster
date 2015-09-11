# vim: foldmethod=marker
from lobster.cmssw import dataset
from lobster import fs, se
import os
import shutil
import subprocess
import tempfile
import unittest

class TestSE(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path = os.path.expandvars('/hadoop/store/user/' + os.environ['USER'] + '/')
        cls.workdir = tempfile.mkdtemp(prefix=path)
        os.chmod(cls.workdir, 0777)
        os.makedirs(os.path.join(cls.workdir, 'spam'))
        for i in range(10):
            with open(os.path.join(cls.workdir, 'spam', str(i) + '.txt'), 'w') as f:
                f.write('eggs')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.workdir)

    def query(self, url):
        if not isinstance(url, list):
            url = [url]
        s = se.StorageConfiguration({'input': url})
        s.activate()

        with fs.default():
            info = dataset.MetaInterface().get_info({'label': 'ham', 'files': 'spam/'})
            assert len(info.files) == 10

    def permissions(self, url):
        if not isinstance(url, list):
            url = [url]
        url = [os.path.join(u, 'bacon') for u in url]

        s = se.StorageConfiguration({'output': url})
        s.activate()

        assert not fs.exists('ham')
        fs.makedirs('ham/eggs')

        parent = os.stat(self.workdir).st_mode
        child = os.stat(os.path.join(self.workdir, 'bacon/ham/eggs')).st_mode

        assert parent == child

class TestLocal(TestSE):
    def runTest(self):
        self.query('file://' + self.workdir)

class TestLocalPermissions(TestSE):
    def runTest(self):
        self.permissions('file://' + self.workdir)

class TestHadoop(TestSE):
    def runTest(self):
        self.query('hdfs://' + self.workdir.replace('/hadoop', '', 1))

class TestHadoopPermissions(TestSE):
    def runTest(self):
        self.permissions('hdfs://' + self.workdir.replace('/hadoop', '', 1))

class TestSRM(TestSE):
    def runTest(self):
        self.query('srm://T3_US_NotreDame' + self.workdir.replace('/hadoop', '', 1))

# gfal-mkdir does not currently support setting permissions
# class TestSRMPermissions(TestSE):
#     def runTest(self):
#         self.permissions('srm://T3_US_NotreDame' + self.workdir.replace('/hadoop', '', 1))

class TestChirp(TestSE):
    def setUp(self):
        fd, self.acl = tempfile.mkstemp()
        with os.fdopen(fd, 'w') as f:
            f.write('unix:' + os.environ['USER'] + ' rlwi\n')
        args=['chirp_server', '-p', '9666',
                '--root=' + self.workdir,
                '-a', 'unix', '-A', self.acl]
        self.p = subprocess.Popen(args)

    def tearDown(self):
        os.unlink(self.acl)
        self.p.terminate()

    def runTest(self):
        self.query('chirp://earth.crc.nd.edu:9666')

class TestChirpPermissions(TestChirp):
    def runTest(self):
        self.permissions('chirp://earth.crc.nd.edu:9666')

class TestFailure(TestSE):
    def runTest(self):
        self.query(['file:///fuckup', 'file://' + self.workdir])

if __name__ == '__main__':
    unittest.main()
