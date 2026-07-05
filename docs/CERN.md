# Running at CERN

These instructions are not for the fainthearted.  Use at your own risk and
customize as needed.

## Setup

Download the CC tools and unpack them in a user-owned location. Follow the
regular Lobster installation procedure. One historical package URL is:

    http://ccl.cse.nd.edu/software/files/cctools-002-b7289441-cvmfs-90fca639c-x86_64-redhat6.tar.gz

Make sure that all elements of the computing chain are using the same
version of the CC tools!

## Setting up your environment

Set the installation directory and adjust your paths as follows:

    export CCTOOLS_DIR=${HOME}/src/cctools-002-b7289441-cvmfs-90fca639c-x86_64-redhat6
    export PYTHONPATH=${CCTOOLS_DIR}/lib/python2.6/site-packages/:${PYTHONPATH}
    export PATH=${PATH}:${CCTOOLS_DIR}/bin

## Actually running

**Be aware that Lobster will stop working as soon as you log out due to the
restrictive AFS permissions at CERN!**

Follow these steps as needed:

1. Start your master and record the lxplus host in `LXPLUS_HOST`.

2. Find the master port by starting a foreman and reading the debug output:

        crcfe01: work_queue_worker -dall --foreman-name validity-1 -M lobster_validity -s /tmp/${USER}
        ...
        2015/06/19 02:59:45.83 work_queue_worker-foreman[60172] dns: 188.184.70.109 is 188.184.70.109
        2015/06/19 02:59:45.83 work_queue_worker-foreman[60172] tcp: connecting to 188.184.70.109:9001
        ...

3. Start SSH forwarding, here from port `9666` at `crcfe01` to `9001` at the recorded lxplus host:

        crcfe01: ssh -L 9666:localhost:9001 ${USER}@${LXPLUS_HOST}.cern.ch -N

4. Start the foreman for real:

        crcfe01: work_queue_worker -dall --foreman-name validity-1 -s /tmp/${USER} localhost 9666

5. Verify in the master and foreman debug logs that they have established a connection.

6. Submit workers:

        crcfe01: cores=4; condor_submit_workers -N validity-1 --cores $cores --memory $(($cores * 900)) --disk $(($cores * 4500)) 25

7. Monitor task progress and the master/foreman logs.
