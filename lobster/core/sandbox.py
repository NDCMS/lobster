from lobster.util import Configurable


class Sandbox(Configurable):

    """
    Parameters
    ----------
        recycle : str
            A path to an existing sandbox to re-use.
        blacklist : list
            A specification of paths to not pack into the sandbox.
    """

    _mutable = {}

    def __init__(self, recycle=None, blacklist=None):
        self.blacklist = blacklist or []
        self.recycle = recycle

    def package(self, basedirs, outdir):
        if self.recycle is not None:
            return self._recycle(outdir)
        return self._package(basedirs, outdir)

    def _package(self, basedirs, outdir):
        pass

    def _recycle(self, outdir):
        pass
