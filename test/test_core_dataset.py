import os
import shutil
import tempfile
import unittest

from lobster.cmssw import dataset
from lobster import fs, se

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

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.workdir)

    def runTest(self):
        s = se.StorageConfiguration({'input': ['file://' + self.workdir]})
        s.activate()

        with fs.default():
            info = dataset.MetaInterface().get_info({'label': 'ham', 'files': 'eggs/'})
            assert len(info.files) == 10

            info = dataset.MetaInterface().get_info({'label': 'ham', 'files': ['eggs/', 'ham/']})
            assert len(info.files) == 15

            info = dataset.MetaInterface().get_info({'label': 'ham', 'files': 'eggs/1.txt'})
            assert len(info.files) == 1
