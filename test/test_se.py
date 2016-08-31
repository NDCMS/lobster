# vim: foldmethod=marker
from lobster.core import dataset
from lobster import fs, se, util
import os
import random
import shutil
import subprocess
import tempfile
import unittest


class TestSE(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        path = os.path.expandvars(
            os.environ.get('LOBSTER_STORAGE', '/hadoop/store/user/') +
            os.environ.get('LOBSTER_USER', os.environ['USER']) + '/')
        if not os.path.exists(path):
            os.makedirs(path)
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
        s = se.StorageConfiguration(output=[], input=url)
        s.activate()

        with util.PartiallyMutable.unlock():
            with fs.default():
                ds = dataset.Dataset(files='spam/')
                info = ds.get_info()
                assert len(info.files) == 10

    def permissions(self, url):
        if not isinstance(url, list):
            url = [url]
        url = [os.path.join(u, 'bacon') for u in url]

        s = se.StorageConfiguration(output=url)
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

if 'LOBSTER_SKIP_HADOOP' not in os.environ:
    class TestHadoop(TestSE):

        def runTest(self):
            self.query('hdfs://' + self.workdir.replace('/hadoop', '', 1))

    class TestHadoopPermissions(TestSE):

        def runTest(self):
            self.permissions(
                'hdfs://' + self.workdir.replace('/hadoop', '', 1))

if 'LOBSTER_SKIP_SRM' not in os.environ:
    class TestSRM(TestSE):

        def runTest(self):
            self.query('srm://T3_US_NotreDame' +
                       self.workdir.replace('/hadoop', '', 1))

    # gfal-mkdir does not currently support setting permissions
    # class TestSRMPermissions(TestSE):
    #     def runTest(self):
    #         self.permissions('srm://T3_US_NotreDame' + self.workdir.replace('/hadoop', '', 1))


class TestChirp(TestSE):

    def setUp(self):
        fd, self.acl = tempfile.mkstemp()
        with os.fdopen(fd, 'w') as f:
            f.write('unix:' + os.environ['USER'] + ' rlwi\n')
        self.port = str(random.randrange(9000, 10000))
        args = ['chirp_server', '-p', self.port,
                '--root=' + self.workdir,
                '-a', 'unix', '-A', self.acl]
        self.p = subprocess.Popen(args)

    def tearDown(self):
        os.unlink(self.acl)
        self.p.terminate()

    def runTest(self):
        self.query('chirp://localhost:' + self.port)


class TestChirpPermissions(TestChirp):

    def runTest(self):
        self.permissions('chirp://localhost:' + self.port)


class TestFailure(TestSE):

    def runTest(self):
        self.query(['file:///fuckup', 'file://' + self.workdir])

if __name__ == '__main__':
    unittest.main()
