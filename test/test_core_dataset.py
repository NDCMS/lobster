import os
import shutil
import tempfile
import unittest

from lobster.core import Dataset
from lobster import fs, se, util


class TestDataset(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        path = os.path.expandvars(
            os.environ.get('LOBSTER_STORAGE', '/hadoop/store/user/') +
            os.environ.get('LOBSTER_USER', os.environ['USER']) + '/')
        if not os.path.exists(path):
            os.makedirs(path)
        cls.workdir = tempfile.mkdtemp(prefix=path)
        os.chmod(cls.workdir, 0777)
        os.makedirs(os.path.join(cls.workdir, 'eggs'))
        for i in range(10):
            with open(os.path.join(cls.workdir, 'eggs', str(i) + '.txt'), 'w') as f:
                f.write('stir-fry')
        os.makedirs(os.path.join(cls.workdir, 'ham'))
        for i in range(5):
            with open(os.path.join(cls.workdir, 'ham', str(i) + '.txt'), 'w') as f:
                f.write('bacon')
        os.makedirs(os.path.join(cls.workdir, 'spam'))
        os.makedirs(os.path.join(cls.workdir, 'spam', 'log'))
        for i in range(5):
            with open(os.path.join(cls.workdir, 'spam', str(i) + '.txt'), 'w') as f:
                f.write('mail')
        for i in range(2):
            with open(os.path.join(cls.workdir, 'spam', str(i) + '.trash'), 'w') as f:
                f.write('mail')
        for i in range(3):
            with open(os.path.join(cls.workdir, 'spam', 'log', str(i) + '.log'), 'w') as f:
                f.write('thing')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.workdir)

    def test_basics(self):
        with util.PartiallyMutable.unlock():
            s = se.StorageConfiguration(
                output=[], input=['file://' + self.workdir])
            s.activate()

            with fs.alternative():
                info = Dataset(files='eggs').get_info()
                assert len(info.files) == 10

                info = Dataset(files=['eggs', 'ham']).get_info()
                assert len(info.files) == 15

                info = Dataset(files='eggs/1.txt').get_info()
                assert len(info.files) == 1

    def test_flatten(self):
        with util.PartiallyMutable.unlock():
            s = se.StorageConfiguration(
                output=[], input=['file://' + self.workdir])
            s.activate()

            with fs.alternative():
                info = Dataset(files=['spam']).get_info()
                assert len(info.files) == 8

                info = Dataset(files=['spam'], patterns=['*.txt']).get_info()
                assert len(info.files) == 5

                info = Dataset(files=['spam'], patterns=['[12].txt']).get_info()
                assert len(info.files) == 2
