**See [special instructions](doc/ND.md) to run at Notre Dame.**

# Installation

Lobster requires Python 2.6.  On SLC/RH 6, the following should be done
after issuing `cmsenv` or equivalent in a release of the `5_3_X` series of
CMSSW.

## Dependencies

### CClab tools

Download the most recent version of the cctools from the [Notre Dame
Cooperative Computing Lab](http://www3.nd.edu/~ccl/software/download.shtml)
and install them with CVMFS (and, for chirp, globus authentication)
enabled.

See [instructions on github](https://github.com/cooperative-computing-lab/cctools)
of the Cooperative Computing Lab to obtain current versions of `parrot` and
`work_queue`.

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

## Setup

Because automatic installation of this dependency may fail, we recommend installing
`hadoopy` manually with

    pip install --user -e git+https://github.com/bwhite/hadoopy#egg=hadoopy

Install lobster itself with

    pip install --user https://github.com/matz-e/lobster/tarball/master

and lobster will be installed as `~/.local/bin/lobster`.

# Running lobster

The following steps are necessary to run lobster (using grid resources for
CMS):

1. Setting up the CMSSW environment (i.e. via `cmsenv`)

2. Obtaining a valid proxy

        voms-proxy-init -voms cms -valid 192:00

3. Download an example configuration file and adjust it for your needs, e.g.:

        wget --no-check-certificate \
            https://raw.githubusercontent.com/matz-e/lobster/master/examples/slim.yaml \
            https://raw.githubusercontent.com/matz-e/lobster/master/examples/slim.py

4. Running lobster

        lobster process slim.yaml

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

6. Starting workers --- see below.

7. Stopping lobster

        lobster terminate slim.yaml

8. Creating summary plots

        lobster plot --outdir <your output directory> <your config/working directory>

   If you have set a `plotdir` in your configuration (at the top level),
   lobster will update plots every 15 minutes by default.

9. Publishing

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

## Stage-out

### Via SRM

Lobster supports SRM stage-out.  Parameters for SRM stage-out can be found
in `/cvmfs/cms.cern.ch/SITECONFIG`, the directory of the destination
storage element, file `PhEDEx/storage.xml`.  A `lfn-to-pfn` leaf with the
`srmv2` protocol will reveal the server URL, which should be used in the
configuration, without any `$1`.  For example, the adjusted settings for
`T3_US_ND` are:

    srm url: "srm://ndcms.crc.nd.edu:8443/srm/v2/server?SFN="
    srm root: ""

For the stage-out, `srm root` is removed from the physical file name and
the result is appended to the `srm url`.

### Via chirp

Using chirp for stage-in and stage-out can be helpful when standard CMS
tools for file handling, i.e., XrootD and SRM, are not available.

Create a file called `acl` with default access permissions in, e.g., your
home directory via: (you will need a valid proxy for this!)

    echo "globus:$(voms-proxy-info -identity|sed 's/ /_/g') rwlda" > ~/acl

On earth, do something akin to the following commands:

    chirp_server --root=<some_directory> -A ~/acl -p <your_port>

where the default port is `9094`, but may be occupied, in which case it
should be best to linearly increment this port until you find a free one.
The `root` directory given to the chirp server can be the desired stage-out
location, or any directory above it.
**If you are using chirp to stage out to a server that cannot handle a high
load, limit the connections by adding `-M 50` to the arguments.**

You should test chirp from another machine:

    voms-proxy-init -voms cms -valid 192:00
    chirp_put <some_file> <your_server>:<your_port> spam

If this command fails with a permission issue, make sure you do not have
any `.__acl` files lingering around in your stageout directory:

    find <your_stageout_directory> -name .__acl -exec rm \{} \;

and try again.

Then add the follow line to your lobster configuration and you should be
all set:

    chirp server: "<your_server>:<your_port>"
    chirp root: <your_directory>

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
