# Dependencies

[PyYaml](http://pyyaml.org/wiki/PyYAML) is a pre-requisite.  Install it
locally after executing `cmsenv` in a release which is suppossed to be used
with lobster:

    cd /tmp
    wget -O - http://pyyaml.org/download/pyyaml/PyYAML-3.10.tar.gz|tar xzf -
    cd PyYAML-3.10/
    python setup.py install --user
    cd ..
    rm -rf PyYAML-3.10/
# Setting up your environment

    export PYTHONPATH=$PYTHONPATH:/afs/nd.edu/user37/ccl/software/cctools-autobuild/lib/python2.7/site-packages/
    export PATH=/afs/nd.edu/user37/ccl/software/cctools-autobuild/bin:$PATH

# Submitting workers

To start 10 workers, 4 cores each, connecing to a lobster instance with id
`chowder`, issue the following commands:

    cores=4
    condor_submit_workers -N lobster_chowder --cores $cores \
        --memory $(($cores * 1100)) --disk $(($cores * 4500)) 10

If the workers get evicted by condor, the memory and disk settings might need
adjustment.  Check in them`lobster.py` for minimum settings (currently 1100 Mb for
memory, 4 Gb for disk).
