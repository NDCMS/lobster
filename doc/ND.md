# Running at ND

These instructions are not to be followed slavishly.  Customization to fit
your setup might be needed.

## Setting up your environment

Use `work_queue` etc from the CC lab:

    export PYTHONPATH=$PYTHONPATH:/afs/nd.edu/user37/ccl/software/cctools/lib/python2.7/site-packages/
    export PATH=/afs/nd.edu/user37/ccl/software/cctools/bin:$PATH

or, for `tcsh` users,

    setenv PYTHONPATH ${PYTHONPATH}:/afs/crc.nd.edu/group/ccl/software/cctools-ndcms/lib/python2.6/site-packages/
    setenv PATH /afs/crc.nd.edu/group/ccl/software/cctools-ndcms/bin:${PATH}

These statements are best put into your shell startup scripts.  Test the
cctools with

    parrot_run ls /

If you see any errors, try replacing `cctools` with `cctools-autobuild`.
Should that also fail, please contact us or the CC lab.

Then enter a `CMSSW` release directory and source the software environment:

    cmsenv

## Installing lobster

Use the following command to install the python setuptools, then proceed as
above:

    wget http://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py -O - | python - --user

To allow you to use the setuptools commands, in tcsh
   
    setenv PATH ~/.local/bin:${PATH}   

Then, to actually install lobster, run

    easy_install https://github.com/matz-e/lobster/tarball/master

to get the most recent version, or

    easy_install https://github.com/matz-e/lobster/archive/v1.0.tar.gz

for the latest stable release.  Then add `.local/bin` to your `PATH`:

    export PATH=$PATH:$HOME/.local/bin

or, for `tcsh` users,

    setenv PATH ${PATH}:$HOME/.local/bin

## Running lobster

### Configuration

Download an example configuration with

    wget --no-check-certificate \
        https://raw.githubusercontent.com/matz-e/lobster/master/examples/beanprod.yaml

and edit it with your favorite editor.  The first two lines specify the ID
the references your lobster instance, and the working directory for log
files and the payload database.

### Running the lobster master

Be sure that you issued

    cmsenv

in the release you want your jobs to run in.  Then obtain a proxy via

    voms-proxy-init -voms cms -valid 192:00

or similar.  Then start lobster:

    lobster process <your_config_file>

Which should print you the location of a log file and stderr.  You can
follow these with

    tail -f <your_working_directory>/lobster.{err,log}

If you see statements regarding failed jobs (exit code >0), some exit codes
are listed [here](ErrorCodes.md).

To stop lobster, use

    lobster terminate <your_config_file/your_working_directory>

And to obtain progress plots:

    lobster plot --outdir <some_web_directory> <your_config_file/your_working_directory>

### Submitting workers opportunistically

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

### Submitting workers locally

To submit 10 workers (= 10 cores) to the T3 at ND, run

    condor_submit_workers -N lobster_<your_id> --cores 1 \
        --memory 1000 --disk 4500 10

on `earth`.

## Advanced usage

### Using chirp

Create a file called `acl` with default access permissions in your home
directory via: (you will need a valid proxy for this!)

    echo "globus:$(voms-proxy-info -identity|sed 's/ /_/g') rwlda" > ~/acl

On earth, do something akin to the following commands:

    chirp_server --root=<your_stageout_directory> -A ~/acl -p <your_port>

where the default port is `9094`, but may be occupied, in which case it
should be best to linearly increment this port until you find a free one.
**If you are using chirp to stage out to `/store`, limit the connections
by adding `-M 50` to the arguments.**

You should test chirp on `ndcms` or any other computer than earth:

    voms-proxy-init -voms cms -valid 192:00
    chirp_put <some_file> earth:<your_port> spam

If this command fails with a permission issue, make sure you do not have
any `.__acl` files lingering around in your stageout directory:

    find <your_stageout_directory> -name .__acl -exec rm \{} \;

and try again.

Then add the follow line to your lobster configuration and you should be
all set:

    chirp server: "earth.crc.nd.edu:<your_port>"

This is optional, but will improve performance.

### Using chirp with hadoop

To run chirp with a direct connection to hadoop, the server command has to
be altered slightly (change to suit your needs):

    cd /var/tmp/
    cp -r /usr/lib/hadoop/ .
    cp /usr/lib64/libhdfs* hadoop/lib/
    env JAVA_HOME=/etc/alternatives/java_sdk/ HADOOP_HOME=$PWD/hadoop chirp_server \
            --root=hdfs://ndcms.crc.nd.edu:19000/<your_stageout_directory_wo_leading_hadoop> \
            -A ~/acl -p <your_port>

Test this command above, and add it to the configuration to enjoy talking
to hadoop directly.

## Monitoring

* [CMS dasboard](http://dashb-cms-job.cern.ch/dashboard/templates/web-job2/)
* [CMS squid statistics](http://wlcg-squid-monitor.cern.ch/snmpstats/indexcms.html)
* [Condor usage](http://condor.cse.nd.edu/condor_matrix.cgi)
* [NDCMS trends](http://mon.crc.nd.edu/xymon-cgi/svcstatus.sh?HOST=ndcms.crc.nd.edu&SERVICE=trends&backdays=0&backhours=6&backmins=0&backsecs=0&Go=Update&FROMTIME=&TOTIME=)
  to monitor squid bandwidth
* [External bandwidth](http://prtg1.nm.nd.edu/sensor.htm?listid=491&timeout=60&id=505&position=0)
* `work_queue_status` on the command line
