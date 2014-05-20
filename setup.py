#!/usr/bin/env python

from setuptools import setup

setup(
    name='Lobster',
    version='0.0.1',
    description='Opportunistic HEP computing tool',
    author='Anna Woodard, Matthias Wolf',
    url='https://github.com/matz-e/lobster',
    packages=['lobster', 'lobster.cmssw'],
    package_data={'lobster': ['cmssw/data/job.py', 'cmssw/data/wrapper.sh']},
    install_requires=[
        'argparse',
        'nose',
        'pyyaml',
        'python-daemon'
    ],
    entry_points={
        'console_scripts': ['lobster = lobster.ui:boil']
    }
)
