import os
import sys

from mock import Mock

sys.modules['ROOT'] = Mock()

from lobster.core.data import task


class TestCommands(object):

    def test_expansion(self):
        cmd = ["foo", "@inputfiles", "--some-flag"]
        args = ["-a"]
        infiles = ["bar", "baz"]
        outfiles = []
        result = ["foo", "bar", "baz", "--some-flag"]
        assert task.expand_command(cmd, args, infiles, outfiles) == result


class TestDiscovery(object):

    def test_xrootd_server(self):
        fn = os.path.join(os.path.dirname(__file__), 'data', 'siteconf', 'PhEDEx', 'storage.xml')
        assert task.find_xrootd_server(fn) == 'root://ndcms.crc.nd.edu/'


class TestXRootDPathJoin(object):

    def test_join_xrootd_path_with_lfn(self):
        assert task.join_xrootd_path(
            'root://cmsxrootd.crc.nd.edu//',
            '/store/mc/file.root'
        ) == 'root://cmsxrootd.crc.nd.edu//store/mc/file.root'


    def test_join_xrootd_path_without_trailing_slash(self):
        assert task.join_xrootd_path(
            'root://cmsxrootd.crc.nd.edu',
            '/store/mc/file.root'
        ) == 'root://cmsxrootd.crc.nd.edu/store/mc/file.root'
