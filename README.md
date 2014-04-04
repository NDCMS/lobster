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

# Running lobster

The following steps are necessary to run lobster (using grid resources for
CMS):

1. Setting up the CMSSW environment (i.e. via `cmsenv`)

2. Obtaining a valid proxy

    voms-proxy-init -voms cms -valid 192:00

3. Adjusting the configuration file, e.g.:

    vi test/beanprod.yaml

4. Running lobster

    ./lobster.py test/beanprod.yaml

5. Starting workers --- see below.

# Submitting workers

To start 10 workers, 4 cores each, connecing to a lobster instance with id
`chowder`, issue the following commands:

    cores=4
    condor_submit_workers -N lobster_chowder --cores $cores \
        --memory $(($cores * 1100)) --disk $(($cores * 4500)) 10

If the workers get evicted by condor, the memory and disk settings might need
adjustment.  Check in them`lobster.py` for minimum settings (currently 1100 Mb for
memory, 4 Gb for disk).

# Monitoring at ND

* [External bandwidth](http://prtg1.nm.nd.edu/sensor.htm?listid=491&timeout=60&id=505&position=0)
* [CMS squid statistics](http://wlcg-squid-monitor.cern.ch/snmpstats/indexcms.html)
* [Condor usage](http://condor.cse.nd.edu/condor_matrix.cgi)
* `work_queue_status` on the command line
