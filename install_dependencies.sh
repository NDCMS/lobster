#!/usr/bin/env sh

if [ -z "$VIRTUAL_ENV" ]; then
	echo "error: no virtualenv defined!"
	exit 1
fi

echo "running on '$(uname -a)'"

dir=$(mktemp -d)
git clone https://github.com/dmwm/DBS.git "$dir"
(
	cd "$dir"
	python setup.py install_system -s dbs-client
	python setup.py install_system -s pycurl-client
)
rm -rf "$dir"
pip install 'git+https://github.com/dmwm/WMCore@1.0.9.patch2#egg=WMCore-1.0.9.patch2'

(
	cd $VIRTUAL_ENV
	wget -O - http://ccl.cse.nd.edu/software/files/cctools-5.2.3-x86_64-redhat7.tar.gz|tar xvzf - --strip-components=1
)
