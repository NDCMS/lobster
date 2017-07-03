Installation
============

.. warning::
   These steps should always be performed in a valid CMSSW environment
   (after executing `cmsenv`)!  Lobster will need python 2.7 or greater
   (but not python 3).  CMSSW version `8_0_15` or above are recommended to
   use.

Prerequisites
-------------

Check you python version after running `cmsenv`::

    $ python -V
    Python 2.7.11

Dependencies
~~~~~~~~~~~~

* CClab tools

  Download the most recent version of the cctools from the `Notre Dame
  Cooperative Computing Lab`_ and install them by unpacking the tarball and
  adding the `bin` directory to your path::

    export cctools lobster-148-c1a7ecbd-cvmfs-0941e442
    wget -O - http://ccl.cse.nd.edu/software/files/${cctools}-x86_64-redhat6.tar.gz
    export PATH=$PWD/${cctools}-x86_64-redhat6/bin:$PATH
    export PYTHONPATH=$PWD/${cctools}-x86_64-redhat6/lib/python2.6/site-packages:$PYTHONPATH

  .. note::
     At Notre Dame, a development version can be accessed via::

      export cctools lobster-148-c1a7ecbd-cvmfs-0941e442
      export PYTHONPATH=$PYTHONPATH:/afs/crc.nd.edu/group/ccl/software/x86_64/redhat6/cctools/$cctools/lib/python2.6/site-packages
      export PATH=/afs/crc.nd.edu/group/ccl/software/x86_64/redhat6/cctools/$cctools/bin:$PATH

     For `tcsh` users these lines have to be adapted [#ftools]_.  You might
     want to add these lines to the shell startup file.

* Setuptools

  Install the python package manager ``pip``, if not already present (may also
  be installed as ``pip2.7``), with::

    wget -O - https://bootstrap.pypa.io/get-pip.py|python - --user

  This installs pip in your `~/.local` directory. In order to access these
  executables, add them to your path with::

    export PATH=$PATH:$HOME/.local/bin

Setup
-----

It is recommended to use lobster in a `virtualenv`, which is used to keep
all dependencies of lobster within one directory and not interfere with
other python packages and their dependencies (e.g. CRAB3)::

    pip install --user virtualenv
    virtualenv --system-site-packages ~/.lobster

And activate the `virtualenv`.  This step has to be done every time lobster
is run, to set the right paths for dependencies::

    . ~/.lobster/bin/activate

To exit the `virtualenv`, use::

    deactivate

Installation as package
~~~~~~~~~~~~~~~~~~~~~~~

Install Lobster with::

    wget -O - https://raw.githubusercontent.com/matz-e/lobster/master/install_dependencies.sh|sh -
    pip install https://github.com/matz-e/lobster/tarball/master

Installation from source
~~~~~~~~~~~~~~~~~~~~~~~~

Lobster can also be installed from a local checkout, which will allow for
easy modification of the source::

    git clone git@github.com:matz-e/lobster.git
    cd lobster
    ./install_dependencies.sh
    pip install --upgrade .

.. _Notre Dame Cooperative Computing Lab: http://www3.nd.edu/~ccl/software/download.shtml

.. rubric:: Footnotes

.. [#ftools] ``tcsh`` users should use the following to access the
   `cctools` development version at Notre Dame::

    setenv cctools lobster-148-c1a7ecbd-cvmfs-0941e442
    setenv PYTHONPATH ${PYTHONPATH}:/afs/crc.nd.edu/group/ccl/software/x86_64/redhat6/cctools/$cctools/lib/python2.6/site-packages
    setenv PATH /afs/crc.nd.edu/group/ccl/software/x86_64/redhat6/cctools/$cctools/bin:${PATH}
