#!/usr/bin/env sh

if [ -z "$CMSSW_BASE" ]; then
 echo "error: must first execute cmsenv!"
 exit
fi

if [ -z "$VIRTUAL_ENV" ]; then
 echo "error: no virtualenv defined!"
 # TODO: check if virtualenv package is installed; if not, install it
 virtualenv --system-site-packages ~/.lobster
 echo '

 if [ -z "$CMSSW_BASE" ]; then
  cd $CMSSW_BASE
  eval $(scramv1 runtime -sh)
  cd -
 fi
 ' >> ~/.lobster/bin/activate
 . ~/.lobster/bin/activate
 pip install --upgrade .
fi

if ! type "parrot_run" > /dev/null; then
 echo "installing cctools"
 cd $VIRTUAL_ENV/src
 # TODO: determine OS and select appropriate tarball
 wget -O - http://ccl.cse.nd.edu/software/files/cctools-6.0.16-source.tar.gz|tar xzf -
 cd cctools*
 ./configure --prefix $VIRTUAL_ENV
 make
 make install
 rm -rf cctools*
fi

