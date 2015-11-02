#!/usr/bin/env python

from setuptools import setup

setup(
    name='Lobster',
    version='1.3',
    description='Opportunistic HEP computing tool',
    author='Anna Woodard, Matthias Wolf',
    url='https://github.com/matz-e/lobster',
    packages=['lobster', 'lobster.cmssw'],
    package_data={'lobster': [
        'core/data/job.py',
        'core/data/wrapper.sh',
        'core/data/mtab',
        'core/data/siteconfig/JobConfig/site-local-config.xml',
        'core/data/siteconfig/PhEDEx/storage.xml',
        'core/data/merge_cfg.py',
        'core/data/merge_reports.py'
        'data/template.html',
    ]},
    install_requires=[
        'argparse',
        'httplib2', # actually a WMCore dependency
        'jinja2',
        'matplotlib',
        'nose',
        'pycurl',
        'pyyaml',
        'python-cjson', # actually a DBS dependency
        'python-daemon',
        'python-dateutil',
        'pytz',
        'requests',
        'retrying',
        'WMCore'
    ],
    dependency_links = [
        'git+https://github.com/dmwm/WMCore@1.0.9.patch2#egg=WMCore-1.0.9.patch2'
    ],
    entry_points={
        'console_scripts': ['lobster = lobster.ui:boil']
    }
)
