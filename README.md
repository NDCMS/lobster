**See special instructions to run at [Notre Dame](doc/ND.md), and
at [CERN](doc/CERN.md).**

# Installation

Lobster requires Python 2.6 or greater (but not Python 3).  CMSSW also uses
Python.  Since you will probably be using Lobster and CMSSW together, you
should make sure that you do `cmsenv` from a CMSSW release in which you
will be using Lobster before running the installation.  (Any release
`5_3_X` or newer should set up an appropriate version of Python.)  After
you run `cmsenv`, to check your python version, use

    python -V

If the version you see is older than Python 2.6, you'll want to get help
from an expert.  Also, if you switch to a release with a signficantly newer
version of Python than you used to install Lobster, you may have to re-run
the Lobster installation instructions below.

## Dependencies

### CClab tools

Download the most recent version of the cctools from the [Notre Dame
Cooperative Computing Lab](http://www3.nd.edu/~ccl/software/download.shtml)
and install them by unpacking the tarball and adding the bin directory to
your path.

    cd $HOME
    wget http://ccl.cse.nd.edu/software/files/cctools-5.0.5-x86_64-redhat6.tar.gz
    tar xvzf cctools-5.0.5-x86_64-redhat6.tar.gz
    export PATH=$HOME/cctools-5.0.5-x86_64-redhat6/bin:$PATH

or, for tcsh

    cd $HOME
    wget http://ccl.cse.nd.edu/software/files/cctools-5.0.5-x86_64-redhat6.tar.gz
    tar xvzf cctools-5.0.5-x86_64-redhat6.tar.gz
    setenv PATH $HOME/cctools-5.0.5-x86_64-redhat6/bin:$PATH

### Setuptools

Install the python `setuptools`, if not already present, with

    wget https://bootstrap.pypa.io/get-pip.py; python get-pip.py --user

This installs pip in your `~/.local` directory. Note that lobster and any further python
dependencies will also be installed there. In order to access these executables, add them
to your path with

    export PATH=$PATH:$HOME/.local/bin

for bash or, for tcsh

    setenv PATH ${PATH}:${HOME}/.local/bin

As you'll need `~/.local` in your path each time you run lobster, we recommend adding
the step above to your login script.

# Setup

Install lobster with

    pip install --user https://github.com/matz-e/lobster/tarball/master

and lobster will be installed as `~/.local/bin/lobster`.

# Running lobster
## Basic procedure
The following steps are necessary to run lobster (using grid resources for
CMS):

1. Setting up the CMSSW environment (i.e. via `cmsenv`)

2. Obtaining a valid proxy

        voms-proxy-init -voms cms -valid 192:00

3. Download an example configuration file and adjust it for your needs, e.g.:

        # reads dataset from CMS DBS and produces slimmed copy
        wget --no-check-certificate \
            https://raw.githubusercontent.com/matz-e/lobster/master/examples/slim_dbs.yaml \
            https://raw.githubusercontent.com/matz-e/lobster/master/examples/slim.py

        # reads input files from specified directory and produces slimmed copy
        wget --no-check-certificate \
            https://raw.githubusercontent.com/matz-e/lobster/master/examples/slim_file_interface.yaml \
            https://raw.githubusercontent.com/matz-e/lobster/master/examples/slim.py

        # produces MC simulation
        wget --no-check-certificate \
            https://raw.githubusercontent.com/matz-e/lobster/master/examples/mc_production.yaml \
            https://raw.githubusercontent.com/matz-e/lobster/master/examples/mc_production.py
            
    For more information on how to specify where the output files should be written,
    see [below](#storage-elements).

4. Running lobster

        lobster process slim_dbs.yaml

   This will start a lobster instance in the background.  Check the logfile
   printed on the terminal for info while running.

   In case of errors, [here](doc/ErrorCodes.md) is an explanation for
   custom exit codes of lobster.

5. Merging output files

   By default, output files are not merged.  If they are too small and
   should be merged, consider adding

        merge size: 300M

   to your configuration.  Then output files will get merged as processing
   jobs finish.  If all processing is already done, only merge jobs will
   run.  Valid units for this option are `K`, `k`, `M`, `m`, `G`, and `g`.

6. Starting workers --- see [below](#submitting-workers).

7. Stopping lobster

        lobster terminate slim_dbs.yaml

8. Creating summary plots

        lobster plot --outdir <your output directory> <your config/working directory>

   If you have set a `plotdir` in your configuration (at the top level),
   lobster will update plots every 15 minutes by default.

9. Publishing

        lobster publish <labels> <your config/working directory>

## Storage elements

Lobster supports multiple file transfer methods:  via Work Queue, Chirp (see
[below](#setting-up-a-chirp-server) for more information), XrootD (input only),
and SRM (output only.)  Configuration is done in terms of paths and URLs that
all point to the same output directory.  Output files are then saved in
sub-directories named after the task labels.

Access to storage elements is defined by a list of access methods that is
traversed until file access is successful. Here is an example for jobs at
Notre Dame:

    storage:
        use work queue for inputs: false   # default is false
        use work queue for outputs: false  # default is false
        disable input streaming: false     # default is false
        shuffle inputs: true               # default is false
        shuffle outputs: true              # default is false

        input:
          - hdfs:///store/user/<user>/<some input directory>
          - file:///hadoop/store/user/<user>/<some input directory>
          - root://ndcms.crc.nd.edu//store/user/<user>/<some input directory>
          - srm://T3_US_NotreDame/store/user/<user>/<some input directory>
          - chirp://<your server>:<your port>/<some input directory>
        output:
          - hdfs:///store/user/<user>/<output directory>
          - file:///hadoop/store/user/<user>/<output directory>
          - root://ndcms.crc.nd.edu//store/user/<user>/<output directory>
          - srm://T3_US_NotreDame/store/user/<user>/<output directory>
          - chirp://<your server>:<your port>/<output directory>

For analysis jobs streaming from external sites via XrootD, `input` should
be empty.  To use the Tier 2 at CERN for storage, use:

    storage:
        input:
          - root://T2_CH_CERN//store/user/<user>/<input directory>
          - srm://T2_CH_CERN//store/user/<user>/<input directory>
        output:
          - root://T2_CH_CERN//store/user/<user>/<output directory>
          - srm://T2_CH_CERN//store/user/<user>/<output directory>

## Submitting workers

To start 10 workers, 4 cores each, connecing to a lobster instance with id
`chowder`, issue the following commands:

    cores=4
    condor_submit_workers -N lobster_chowder --cores $cores \
        --memory $(($cores * 1100)) --disk $(($cores * 4500)) 10

If the workers get evicted by condor, the memory and disk settings might need
adjustment.  Check in them`lobster.py` for minimum settings (currently 1100 Mb for
memory, 4 Gb for disk).

## Using foremen

To lessen the load on the master spooling out sandboxes and handling
communication with workers, foreman can be used.  They can be started with,
e.g.:

    work_queue_worker -olog_foreman_<n> -dall --foreman-name \
        lobster-foreman-<n> -M lobster_chowder -s /tmp/<some_directory> --specify-log \
        foreman_<n>.log --timeout=86400

Prepending `nohup` to the command keeps it from getting killed when the
current shell or ssh session is closed.  Alternatively, it is also possible
to run the foremen in a `screen` or `tmux` session.

## Using `work_queue_pool`

When using foremen, `work_queue_pool` can be used to relieve the user of
the manual management of workers, and distribute them evenly between
foremen.  Run the following command to create a minimum of 100 workers, and a
maximum of 200 workers for a lobster master running with formen:

    work_queue_pool -T condor -o work_queue_pool.log \
        -M lobster_<your id> -F "lobster-foreman.*" -w 100 -W 200 \
        --cores $cores --memory $(($cores * 1100)) --disk $(($cores * 4500))

The last line of arguments corresponds to the desired worker configuration.

# Monitoring

* [CMS dasboard](http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/)
* [CMS squid statistics](http://wlcg-squid-monitor.cern.ch/snmpstats/indexcms.html)

# Advanced usage
## Setting up a Chirp server

Using Chirp for stage-in and stage-out can be helpful when standard CMS
tools for file handling, i.e., XrootD and SRM, are not available.

Create a file called `acl` with default access permissions in, e.g., your
home directory via: (you will need a valid proxy for this!)

    echo "globus:$(voms-proxy-info -identity|sed 's/ /_/g') rwlda" > ~/acl

On earth, do something akin to the following commands:

    chirp_server --root=<some_directory> -A ~/acl -p <your_port>

where the default port is `9094`, but may be occupied, in which case it
should be best to linearly increment this port until you find a free one.
The `root` directory given to the Chirp server can be the desired stage-out
location, or any directory above it.
**If you are using Chirp to stage out to a server that cannot handle a high
load, limit the connections by adding `-M 50` to the arguments.**

You should test Chirp from another machine:

    voms-proxy-init -voms cms -valid 192:00
    chirp_put <some_file> <your_server>:<your_port> spam

If this command fails with a permission issue, make sure you do not have
any `.__acl` files lingering around in your stageout directory:

    find <your_stageout_directory> -name .__acl -exec rm \{} \;

and try again.

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
