#!/usr/bin/env python

import os

from lobster import das_interface, sandbox

sandbox.package(os.environ['LOCALRT'], 'cmssw.tar.bz2')
