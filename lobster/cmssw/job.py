import base64
import os
import pickle
import shutil

import lobster.job
import sandbox

from das import DASInterface
from jobit import SQLInterface as JobitStore

class JobProvider(lobster.job.JobProvider):
    def __init__(self, config):
        self.__config = config

        self.__workdir = config['workdir']
        self.__sandbox = os.path.join(self.__workdir, 'sandbox')

        self.__labels = {}
        self.__configs = {}

        das = DASInterface()

        if not os.path.exists(self.__workdir):
            os.makedirs(self.__sandbox)

            self.__store = JobitStore(config)

            shutil.copy(os.path.join(os.path.dirname(__file__), 'data', 'job.py'),
                    os.path.join(self.__sandbox, 'job.py'))

            sandbox.package(os.environ['LOCALRT'], self.__sandbox)

            self.__store.register_jobits(das)

            for cfg in config['tasks']:
                label = cfg['dataset label']
                cms_config = cfg['cmssw config']

                taskdir = os.path.join(self.__workdir, label)
                if not os.path.exists(taskdir):
                    os.makedirs(taskdir)

                shutil.copy(cms_config, os.path.join(taskdir, os.path.basename(cms_config)))
        else:
            self.__store = JobitStore(config)
            self.__store.reset_jobits()

        for cfg in config['tasks']:
            label = cfg['dataset label']
            cms_config = cfg['cmssw config']

            self.__labels[cfg['dataset']] = label
            self.__configs[label] = os.path.basename(cms_config)

    def obtain(self):
        (id, dataset, files, lumis) = self.__store.pop_jobits(5)

        print "Creating job", id

        label = self.__labels[dataset]
        config = self.__configs[label]
        args = "" # FIXME

        inputs = [(os.path.join(self.__workdir, label, config), config)]
        for entry in os.listdir(self.__sandbox):
            inputs.append((os.path.join(self.__sandbox, entry), entry))

        jdir = os.path.join(self.__workdir, label, 'running', id)
        outputs = [
            (os.path.join(jdir, 'cmssw.out'), 'cmssw.out'),
            (os.path.join(jdir, 'cmssw.err'), 'cmssw.err'),
            (os.path.join(jdir, 'report.xml'), 'report.xml')
            ]

        cmd = "python job.py {0} {1} {2}".format(
            config,
            base64.b64encode(pickle.dumps((files, lumis))),
            base64.b64encode(args))

        return (id, cmd, inputs, outputs)

    def release(self, id, return_code):
        pass

    def done(self):
        pass
