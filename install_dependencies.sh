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

if [ -z "$BUILD_CCTOOLS" ]; then
	exit 0
fi

(
	cd $VIRTUAL_ENV/src
	wget -O - http://ccl.cse.nd.edu/software/files/cctools-lobster-154-16b67876-cvmfs-70dfa0d6-source.tar.gz|tar xzf -
	cd cctools*
	sed -i 's/\(config_perl_path\)=auto/\1=no/' ./configure
	./configure --prefix $VIRTUAL_ENV
	make
	make install
)
