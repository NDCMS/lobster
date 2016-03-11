# Running at CERN

These instructions are not for the fainthearted.  Use at your own risk and
customize as needed.

## Setup

Download the CC tools and unpack them somewhere.  Follow the regular
Lobster installation procedure.  I used:

    http://ccl.cse.nd.edu/software/files/cctools-002-b7289441-cvmfs-90fca639c-x86_64-redhat6.tar.gz

Make sure that all elements of the computing chain are using the same
version of the CC tools!

## Setting up your environment

Adjust your paths as follows:

    export PYTHONPATH=/afs/cern.ch/user/m/matze/src/cctools-002-b7289441-cvmfs-90fca639c-x86_64-redhat6/lib/python2.6/site-packages/:$PYTHONPATH
    export PATH=$PATH:/afs/cern.ch/user/m/matze/src/cctools-002-b7289441-cvmfs-90fca639c-x86_64-redhat6/bin

## Actually running

**Be aware that Lobster will stop working as soon as you log out due to the
restrictive AFS permissions at CERN!**

Follow these steps as needed:

1. Start your master.  Note the host you are running on.  Mine was `lxplus0076`.

2. Find out your master port.  I tried to start a foreman and got the port from the debug output:

        crcfe01: work_queue_worker -dall --foreman-name validity-1 -M lobster_validity -s /tmp/matze
        ...
        2015/06/19 02:59:45.83 work_queue_worker-foreman[60172] dns: 188.184.70.109 is 188.184.70.109
        2015/06/19 02:59:45.83 work_queue_worker-foreman[60172] tcp: connecting to 188.184.70.109:9001
        ...

3. Start the ssh forwarding, here from port `9666` at `crcfe01` to `9001` at `lxplus0076`:

        crcfe01: ssh -L 9666:localhost:9001 matze@lxplus0076.cern.ch -N

4. Start the foreman for real:

        crcfe01: work_queue_worker -dall --foreman-name validity-1 -s /tmp/matze localhost 9666

5. Verify in the master and foreman debug logs that they have established a connection.

6. Submit workers:

        crcfe01: cores=4; condor_submit_workers -N validity-1 --cores $cores --memory $(($cores * 900)) --disk $(($cores * 4500)) 25

7. Profit!
