#!/usr/bin/env python

from distutils.core import setup

setup(name='Lobster',
        version='0',
        description='Opportunistic HEP computing tool',
        author='Anna Woodard, Matthias Wolf',
        url='https://github.com/matz-e/lobster',
        packages=['lobster'],
        package_data={'lobster': ['data/job.py']}
        scripts=['lobster.py'],
        )
