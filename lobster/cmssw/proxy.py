import logging
import os
import time

from WMCore.Credential.Proxy import Proxy as WMProxy
from lobster.util import Configurable, PartiallyMutable


logger = logging.getLogger('lobster.cmssw.proxy')


class Proxy(Configurable):

    """
    Wrapper around CMS credentials.

    Parameters
    ----------
        renew : bool
            Whether to renew proxy.  Defaults to `True`.
    """

    _mutable = {}

    def __init__(self, renew=True):
        self.renew = renew
        self.__proxy = WMProxy({'logger': logging.getLogger("WMCore"), 'proxyValidity': '192:00'})
        self.__setup()

    def __setup(self):
        if self.check() and self.__proxy.getTimeLeft() > 4 * 3600:
            if 'X509_USER_PROXY' not in os.environ:
                os.environ['X509_USER_PROXY'] = self.__proxy.getProxyFilename()
        elif self.renew:
            self.__proxy.renew()
            if self.__proxy.getTimeLeft() < 4 * 3600:
                raise AttributeError("could not renew proxy")
            os.environ['X509_USER_PROXY'] = self.__proxy.getProxyFilename()
        else:
            raise AttributeError("please renew or disable your proxy")

    def __getstate__(self):
        state = dict(self.__dict__)
        del state['_Proxy__proxy']
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        with PartiallyMutable.unlock():
            self.__proxy = WMProxy({'logger': logging.getLogger("WMCore"), 'proxyValidity': '192:00'})
            self.__setup()

    def check(self):
        left = self.__proxy.getTimeLeft()
        if left == 0:
            return False
        elif left < 4 * 3600:
            logger.warn("only {0}:{1:02} left in proxy lifetime!".format(left / 3600, left / 60))
        return True

    def expires(self):
        return int(time.time()) + self.__proxy.getTimeLeft()
