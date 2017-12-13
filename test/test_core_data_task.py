import os

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
