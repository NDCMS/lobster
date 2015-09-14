#!/usr/bin/env python

from setuptools import setup

setup(
    name='Lobster',
    version='1.2',
    description='Opportunistic HEP computing tool',
    author='Anna Woodard, Matthias Wolf',
    url='https://github.com/matz-e/lobster',
    packages=['lobster', 'lobster.cmssw'],
    package_data={'lobster': [
        'cmssw/data/job.py',
        'cmssw/data/wrapper.sh',
        'cmssw/data/mtab',
        'cmssw/data/siteconfig/JobConfig/site-local-config.xml',
        'cmssw/data/siteconfig/PhEDEx/storage.xml',
        'cmssw/data/template.html',
        'cmssw/data/merge_cfg.py',
        'cmssw/data/merge_reports.py'
    ]},
    install_requires=[
        'argparse',
        'dbs',
        'jinja2',
        'nose',
        'pyyaml',
        'python-daemon',
        'python-dateutil',
        'pytz',
        'WMCore'
    ],
    dependency_links = [
        'git+https://github.com/dmwm/DBS@DBS_3_2_114#egg=dbs-3.2.114',
        'git+https://github.com/dmwm/WMCore@1.0.9.patch2#egg=WMCore-1.0.9.patch2'
    ],
    entry_points={
        'console_scripts': ['lobster = lobster.ui:boil']
    }
)
