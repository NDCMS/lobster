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
        self.__args = {}
        self.__jobdirs = {}

        das = DASInterface()

        if not os.path.exists(self.__workdir):
            os.makedirs(self.__sandbox)

            self.__store = JobitStore(config)

            for fn in ['job.py', 'wrapper.sh']:
                shutil.copy(os.path.join(os.path.dirname(__file__), 'data', fn),
                        os.path.join(self.__sandbox, fn))

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
            self.__args[label] = cfg['parameters']

    def obtain(self):
        (id, dataset, files, lumis) = self.__store.pop_jobits(5)

        print "Creating job", id

        label = self.__labels[dataset]
        config = self.__configs[label]
        args = self.__args[label]

        inputs = [(os.path.join(self.__workdir, label, config), config)]
        for entry in os.listdir(self.__sandbox):
            inputs.append((os.path.join(self.__sandbox, entry), entry))

        jdir = os.path.join(self.__workdir, label, 'running', id)
        if not os.path.isdir(jdir):
            os.makedirs(jdir)

        with open(os.path.join(jdir, 'parameters.pkl'), 'wb') as f:
            pickle.dump((args, files, lumis), f, pickle.HIGHEST_PROTOCOL)
        inputs.append((os.path.join(jdir, 'parameters.pkl'), 'parameters.pkl'))

        self.__jobdirs[id] = jdir

        outputs = [
            (os.path.join(jdir, 'cmssw.log'), 'cmssw.log'),
            (os.path.join(jdir, 'report.xml'), 'report.xml')
            ]

        cmd = './wrapper.sh python job.py {0} parameters.pkl'.format(config)

        return (id, cmd, inputs, outputs)

    def release(self, id, return_code, output):
        print "Job", id, "returned with exit code", return_code

        failed = (return_code != 0)
        self.__store.update_jobits(id, failed)

        jdir = self.__jobdirs[id]

        with open(os.path.join(jdir, 'job.log'), 'w') as f:
            f.write(output)

        if failed:
            shutil.move(jdir, jdir.replace('running', 'failed'))
        else:
            shutil.move(jdir, jdir.replace('running', 'successful'))

    def done(self):
        return self.__store.unfinished_jobits() == 0

    def work_left(self):
        return self.__store.unfinished_jobits()
