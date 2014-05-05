# Installation

## Dependencies

### CClab tools

See [instructions on github](https://github.com/cooperative-computing-lab/cctools)
of the Notre Dame Cooperative Computing Lab to obtain versions of
`parrot` and `work_queue`.

### Setuptools

Install the python `setuptools`, if not already present, with

    wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O - | python - --user

Now `lobster` can be installed, and any further python dependencies will be
installed into your `~/.local` directory.

## Setup

Install lobster itself with

    git clone git@github.com:matz-e/lobster.git
    cd lobster
    python setup.py install --user

and `lobster` will be installed as `~/.local/bin/lobster`.  Add it to your
path with

    export PATH=$PATH:$HOME/.local/bin

# Running lobster

The following steps are necessary to run lobster (using grid resources for
CMS):

1. Setting up the CMSSW environment (i.e. via `cmsenv`)

2. Obtaining a valid proxy

        voms-proxy-init -voms cms -valid 192:00

3. Adjusting the configuration file, e.g.:

        vi test/beanprod.yaml

4. Running lobster

        lobster process test/beanprod.yaml

   This will start a lobster instance in the background.  Check the logfile
   printed on the terminal for info while running.

   To stop lobster, you will need to find its PID and kill it manually.
   Also check that there is no stale lock file, i.e.,
   `<workdir>/lobster.pid.lock`, which will keep lobster from running with
   more than one instance at any time.

5. Starting workers --- see below.

6. Creating summary plots

        lobster plot --outdir <your output directory> <your config/working directory>

7. Publishing

        lobster publish <labels> <your config/working directory>

# Submitting workers

To start 10 workers, 4 cores each, connecing to a lobster instance with id
`chowder`, issue the following commands:

    cores=4
    condor_submit_workers -N lobster_chowder --cores $cores \
        --memory $(($cores * 1100)) --disk $(($cores * 4500)) 10

If the workers get evicted by condor, the memory and disk settings might need
adjustment.  Check in them`lobster.py` for minimum settings (currently 1100 Mb for
memory, 4 Gb for disk).

# Running at ND

## Setting up your environment

Use `work_queue` etc from the CC lab:

    export PYTHONPATH=$PYTHONPATH:/afs/nd.edu/user37/ccl/software/cctools-lobster/lib/python2.7/site-packages/
    export PATH=/afs/nd.edu/user37/ccl/software/cctools-lobster/bin:$PATH

or, for `tcsh` users,

    setenv PYTHONPATH ${PYTHONPATH}:/afs/nd.edu/user37/ccl/software/cctools-lobster/lib/python2.7/site-packages/
    setenv PATH /afs/nd.edu/user37/ccl/software/cctools-lobster/bin:${PATH}

## Installing `lobster`

Use the following command to install the python setuptools, then proceed as
above:

    wget --no-check-certificate https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O - | python - --user

## Running opportunistically

The CRC login nodes `opteron`, `newcell`, and `crcfe01` are connected to
the ND opportunistic computing pool.  On these, multicore jobs are
preferred and can be run with

    cores=4
    condor_submit_workers -N lobster_<your_id> --cores $cores \
        --memory $(($cores * 1100)) --disk $(($cores * 4500)) 10

or, for `tcsh` users,

    set cores=4
    condor_submit_workers -N lobster_<your_id> --cores $cores \
        --memory `dc -e "$cores 1100 *p"` --disk `dc -e "$cores 4500 *p"` 10

## Running locally

To submit 10 workers (= 10 cores) to the T3 at ND, run

    condor_submit_workers -N lobster_<your_id> --cores 1 \
        --memory 1000 --disk 4500 10

on `earth`.

## Monitoring

* [CMS dasboard](http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/)
* [CMS squid statistics](http://wlcg-squid-monitor.cern.ch/snmpstats/indexcms.html)
* [Condor usage](http://condor.cse.nd.edu/condor_matrix.cgi)
* [NDCMS trends](http://mon.crc.nd.edu/xymon-cgi/svcstatus.sh?HOST=ndcms.crc.nd.edu&SERVICE=trends&backdays=0&backhours=6&backmins=0&backsecs=0&Go=Update&FROMTIME=&TOTIME=)
  to monitor squid bandwidth
* [External bandwidth](http://prtg1.nm.nd.edu/sensor.htm?listid=491&timeout=60&id=505&position=0)
* `work_queue_status` on the command line
