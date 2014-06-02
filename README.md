**See [special instructions](doc/ND.md) to run at Notre Dame.**

# Installation

Lobster requires Python 2.6.  On SLC/RH 5, the following should be done
after issuing `cmsenv` or equivalent in a release of the `5_3_X` series of
CMSSW.

## Dependencies

### CClab tools

Download version 4.2.0rc1 of the cctools from the [Notre Dame Cooperative
Computing Lab](http://www3.nd.edu/~ccl/software/download.shtml) and install
them with CVMFS (and, for chirp, globus authentication) enabled.

See [instructions on github](https://github.com/cooperative-computing-lab/cctools)
of the Cooperative Computing Lab to obtain current versions of `parrot` and
`work_queue`.

### Setuptools

Install the python `setuptools`, if not already present, with

    wget https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O - | python - --user

Now lobster can be installed, and any further python dependencies will be
installed into your `~/.local` directory.

## Setup

Install lobster itself with

    easy_install https://github.com/matz-e/lobster/tarball/master

and lobster will be installed as `~/.local/bin/lobster`.  Add it to your
path with

    export PATH=$PATH:$HOME/.local/bin

# Running lobster

The following steps are necessary to run lobster (using grid resources for
CMS):

1. Setting up the CMSSW environment (i.e. via `cmsenv`)

2. Obtaining a valid proxy

        voms-proxy-init -voms cms -valid 192:00

3. Adjusting the configuration file, e.g.:

        vi examples/beanprod.yaml

4. Running lobster

        lobster process examples/beanprod.yaml

   This will start a lobster instance in the background.  Check the logfile
   printed on the terminal for info while running.

5. Starting workers --- see below.

6. Stopping lobster

        lobster terminate examples/beanprod.yaml

7. Creating summary plots

        lobster plot --outdir <your output directory> <your config/working directory>

8. Publishing

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

## Altering the worker environment

The following environment variables may be set to influence the environment
of the job, and may need adjusting to run on the site:
<dl>
  <dt>PARROT_PATH</dt>
  <dd>The path of <code>parrot_run</code>.  Default is to look
  <code>parrot_run</code> in the <code>PATH</code> environment variable
  where lobster is started and send this version to the worker.  If another
  parrot binary is preferred, set this environment variable to the
  corresponding directory.</dd>

  <dt>PARROT_DEBUG_FLAGS</dt>
  <dd>Which debug flags to use.  Default are none.  See the <code>parrot_run</code>
  help for more details.</dd>
</dl>

To receive CVMFS debug messages from parrot, alter your environment with
the following command **before** submitting workers:

    export PARROT_DEBUG_FLAGS=cvmfs

# Monitoring

* [CMS dasboard](http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/)
* [CMS squid statistics](http://wlcg-squid-monitor.cern.ch/snmpstats/indexcms.html)
