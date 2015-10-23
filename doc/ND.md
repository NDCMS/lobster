# Running at ND

These instructions are not to be followed slavishly.  Customization to fit
your setup might be needed.

## Setting up your environment

Use `work_queue` etc from the CC lab:

    cctools=cctools-033-fa20fc6e-cvmfs-40cf5bba
    export PYTHONPATH=$PYTHONPATH:/afs/crc.nd.edu/group/ccl/software/$cctools/x86_64/redhat6/lib/python2.6/site-packages
    export PATH=/afs/crc.nd.edu/group/ccl/software/$cctools/x86_64/redhat6/bin:$PATH

or, for `tcsh` users,

    setenv cctools-033-fa20fc6e-cvmfs-40cf5bba
    setenv PYTHONPATH ${PYTHONPATH}:/afs/crc.nd.edu/group/ccl/software/$cctools/x86_64/redhat6/lib/python2.6/site-packages
    setenv PATH /afs/crc.nd.edu/group/ccl/software/$cctools/x86_64/redhat6/bin:${PATH}

These statements are best put into your shell startup scripts.  Test the
cctools with

    env PARROT_ALLOW_SWITCHING_CVMFS_REPOSITORIES=TRUE HTTP_PROXY=` awk -F = '/PROXY/ {print $2}' /etc/cvmfs/default.local | sed 's/"//g' ` parrot_run ls /

If you see any errors, try replacing `cctools` with `cctools-autobuild`.
Should that also fail, please contact us or the CC lab.

Then enter a `CMSSW` release directory and source the software environment:

    cmsenv

### Submitting workers opportunistically

The CRC login nodes `opteron`, `newcell`, and `crcfe01` are connected to
the ND opportunistic computing pool.  On these, multicore jobs are
preferred and can be run with the same commands as specified in the main
documentation.

## Advanced usage

### Using chirp with hadoop

To run chirp with a direct connection to hadoop, the server command has to
be altered slightly (change to suit your needs):

    cd /var/tmp/
    cp -r /usr/lib/hadoop/ .
    cp /usr/lib64/libhdfs* hadoop/lib/
    env JAVA_HOME=/etc/alternatives/java_sdk/ HADOOP_HOME=$PWD/hadoop LIBHDFS_OPTS=-Xmx100m chirp_server \
            --root=hdfs://ndcms.crc.nd.edu:19000/<your_stageout_directory_wo_leading_hadoop> \
            -A ~/acl -p <your_port>

Test this command above, and add it to the configuration to enjoy talking
to hadoop directly.

## Notre Dame Monitoring

* [Condor usage](http://condor.cse.nd.edu/condor_matrix.cgi)
* [NDCMS trends](http://mon.crc.nd.edu/xymon-cgi/svcstatus.sh?HOST=ndcms.crc.nd.edu&SERVICE=trends&backdays=0&backhours=6&backmins=0&backsecs=0&Go=Update&FROMTIME=&TOTIME=)
  to monitor squid bandwidth
* [External bandwidth](http://prtg1.nm.nd.edu/sensor.htm?listid=491&timeout=60&id=505&position=0)
* `work_queue_status` on the command line
