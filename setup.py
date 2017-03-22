#!/usr/bin/env python

import os

from setuptools import setup
from setuptools.command.install import install

from lobster.util import get_version


class Install(install):

    def run(self):
        os.system("./install_dependencies.sh")
        install.run(self)


setup(
    name='Lobster',
    version=get_version(),
    description='Opportunistic HEP computing tool',
    author='Anna Woodard, Matthias Wolf',
    url='https://github.com/matz-e/lobster',
    cmdclass={'install': Install},
    packages=[
        'lobster',
        'lobster.cmssw',
        'lobster.cmssw.commands',
        'lobster.core',
        'lobster.commands',
        'lobster.monitor',
        'lobster.monitor.elk'
    ],
    package_data={'lobster': [
        'core/data/task.py',
        'core/data/wrapper.sh',
        'core/data/mtab',
        'core/data/siteconf/JobConfig/site-local-config.xml',
        'core/data/siteconf/PhEDEx/storage.xml',
        'core/data/merge_cfg.py',
        'core/data/merge_reports.py',
        'commands/data/index.html',
        'commands/data/gh.png',
        'commands/data/styles.css',
        'commands/data/category.html',
        'monitor/elk/data/index/*.json',
        'monitor/elk/data/dash/*.json',
        'monitor/elk/data/vis/*.json',
        'monitor/elk/data/*.json'
    ]},
    install_requires=[
        'argparse',
        'elasticsearch',
        'elasticsearch_dsl',
        'httplib2',  # actually a WMCore dependency
        'jinja2',
        'matplotlib',
        'nose',
        'numpy>=1.9',
        'psutil',
        'python-cjson',  # actually a DBS dependency
        'python-daemon',
        'python-dateutil',
        'pytz',
        'pyxdg',
        'requests',
        'retrying',
        'snakebite<2.0',
        'WMCore'
    ],
    dependency_links=[
        'git+https://github.com/dmwm/WMCore@1.0.9.patch2#egg=WMCore-1.0.9.patch2'
    ],
    entry_points={
        'console_scripts': ['lobster = lobster.ui:boil']
    }
)
