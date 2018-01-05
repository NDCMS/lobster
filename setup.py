#!/usr/bin/env python

from setuptools import setup

from lobster.util import get_version

setup(
    name='Lobster',
    version=get_version(),
    description='Opportunistic HEP computing tool',
    author='Anna Woodard, Matthias Wolf',
    url='https://github.com/matz-e/lobster',
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
        'core/data/report.json.in',
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
        'elasticsearch>=5.0.0,<6.0.0',
        'elasticsearch_dsl>=5.0.0,<6.0.0',
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
        'wmcore>=1.1.1.pre7'
    ],
    entry_points={
        'console_scripts': ['lobster = lobster.ui:boil']
    }
)
