import os
import unittest

from lobster.core.task import TaskHandler
from lobster.core.source import ReleaseSummary

class DummyTask(object):
    def __init__(self, tag=1, exitcode=0, result=0):
        self.tag = tag
        self.return_status = exitcode
        self.result = result
        self.output = None
        self.hostname = 'fake'

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        return 0

class TestTask(unittest.TestCase):
    def test_output_info(self):
        summary = ReleaseSummary()
        handler = TaskHandler(1, "test", [], [], [],
                os.path.join(os.path.dirname(__file__), "data/handler/successful"))
        failed, task_update, file_update, unit_update = handler.process(DummyTask(), summary)
        outinfo = handler.output_info
        assert outinfo.lumis == [(1, 261), (1, 262), (1, 263), (1, 264), (1, 265), (1, 266), (1, 267),
                (1, 268), (1, 269), (1, 270), (1, 271), (1, 272), (1, 273), (1, 274), (1, 275),
                (1, 276), (1, 277), (1, 278), (1, 279), (1, 280)]
        assert outinfo.events == 4000
        assert outinfo.size == 15037503
