from types import SimpleNamespace
import unittest
from unittest import mock

from lobster import se


class TestXrootDExecute(unittest.TestCase):

    def setUp(self):
        self.xrootd = se.XrootD('root://example.org/store/user')
        self.path = 'root://example.org/store/user/output.root'

    def run_result(self, returncode=0, stdout='', stderr=''):
        return SimpleNamespace(returncode=returncode, stdout=stdout, stderr=stderr)

    def test_nonzero_unsafe_raises_ioerror_with_diagnostics(self):
        with mock.patch('lobster.se.subprocess.run') as run:
            run.return_value = self.run_result(
                returncode=42, stdout='stdout details', stderr='stderr details')

            with self.assertRaises(IOError) as raised:
                self.xrootd.execute('rm', self.path)

        message = str(raised.exception)
        self.assertIn('xrdfs example.org rm /store/user/output.root', message)
        self.assertIn('return code 42', message)
        self.assertIn('stderr details', message)
        self.assertIn('stdout details', message)
        self.assertNotIsInstance(raised.exception, NameError)

    def test_nonzero_safe_does_not_raise(self):
        with mock.patch('lobster.se.subprocess.run') as run:
            run.return_value = self.run_result(returncode=42, stdout='safe stdout', stderr='safe stderr')

            output = self.xrootd.execute('stat', self.path, safe=True)

        self.assertEqual(output, 'safe stdout')

    def test_zero_returncode_preserves_stdout_output(self):
        with mock.patch('lobster.se.subprocess.run') as run:
            run.return_value = self.run_result(returncode=0, stdout='Size: 1\n', stderr='')

            output = self.xrootd.execute('stat', self.path)

        self.assertEqual(output, 'Size: 1\n')

    def test_remove_nonzero_unsafe_raises_ioerror_not_nameerror(self):
        with mock.patch.object(self.xrootd, 'isdir', return_value=False):
            with mock.patch('lobster.se.subprocess.run') as run:
                run.return_value = self.run_result(
                    returncode=7, stdout='remove stdout', stderr='remove stderr')

                with self.assertRaises(IOError) as raised:
                    self.xrootd.remove(self.path)

        message = str(raised.exception)
        self.assertIn('xrdfs example.org rm /store/user/output.root', message)
        self.assertIn('return code 7', message)
        self.assertIn('remove stderr', message)
        self.assertIn('remove stdout', message)
        self.assertNotIsInstance(raised.exception, NameError)


if __name__ == '__main__':
    unittest.main()
