#!/usr/bin/env sh

dir=$(mktemp -d)
git clone git@github.com:dmwm/DBS.git "$dir"
(
	cd "$dir"
	python setup.py install_system -s dbs-client
	python setup.py install_system -s pycurl-client
)
rm -rf "$dir"
