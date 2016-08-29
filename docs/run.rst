Running
=======

.. note::
   These steps should always be performed in a valid CMSSW environment
   (after executing `cmsenv`), and with the `virtualenv` activated!

After :ref:`configuring Lobster <config>`, and ensuring that both the CMSSW
environment and the `virtualenv` are loaded, the project can be processed.

Basic procedure
---------------

This is the most basic usage of Lobster.  For more information about the
commands, use ``lobster -h`` and ``lobster <cmd> -h``.

1. Obtain a valid proxy::

    voms-proxy-init -voms cms -valid 192:00

2. Start Lobster::

    lobster process config.py

3. Submit workers, see also submission_::

    label=shuffle
    cores=4
    condor_submit_workers -N lobster_$label --cores $cores \
        --memory $(($cores * 1100)) --disk $(($cores * 4500)) 10

4. Follow the log output in the working directory of the
   :class:`~lobster.core.Config`.  Log output can be found in
   `process.log`, and the error output in `process.err`::

    tail -f /my/working/directory/process.{err,log}

5. Validate the output directory to catch any stray or missing files::

    lobster validate --dry-run config.py

   and, after verifying the printout from the above, run it again without
   the ``--dry-run`` argument.

.. _submission:
Submitting workers
------------------

Workers can be either submitted directly, as shown above, or by using the
`WorkQueue` factory, which allows for dynamic scaling of the processing.
The factory uses a config written in JSON, like:

.. literalinclude:: ../examples/factory.json
   :language: json

This configuration sets up 4 core workers providing 4.4 GB of RAM and 12 GB
of disk space each.  It is included in the `examples` directory in the
Lobster source distribution.  The factory can thus be started from within
the Lobster source directory with::

    nohup work_queue_factory -T condor -M "lobster_$USER.*" -dall -o /tmp/${USER}_factory.debug -C examples/factory.json > /tmp/${USER}_factory.log &

If the log of the factory grows too large, removing the ``-dall`` will
disable debug output, and considerably lessen disk usage.

.. note::
   At Notre Dame, the following login nodes are connected to the
   opportunistic resource: `crcfe01`, `crcfe02`, and `condorfe`.  The
   resources have monitoring pages for `condor`_ and the `external
   bandwidth`_, and the `squid server`_.

.. _condor: http://condor.cse.nd.edu/condor_matrix.cgi
.. _external bandwidth: http://prtg1.nm.nd.edu/sensor.htm?listid=491&timeout=60&id=505&position=0
.. _squid server: http://mon.crc.nd.edu/xymon-cgi/svcstatus.sh?HOST=eddie.crc.nd.edu&SERVICE=trends&backdays=0&backhours=6&backmins=0&backsecs=0&Go=Update&FROMTIME=&TOTIME=

Using a Chirp server
--------------------

Using Chirp for stage-in and stage-out can be helpful when standard CMS
tools for file handling, i.e., XrootD and SRM, are not available.

At Notre Dame, a Chirp server is running under `eddie.crc.nd.edu:9094`.
Make sure that your output directory can be reached with globus
authentication by looking for a line starting with ``globus:`` that should
match your personal information in::

    cat /hadoop/store/user/$USER/.__acl

If this line is not present or the file does not exist, create it with::

    echo "globus:`voms-proxy-info -identity|sed 's/ /_/g'` rwlda" >> /hadoop/store/user/$USER/.__acl

Then verify that you can write to your output directory::

    chirp eddie.crc.nd.edu:9094 mkdir /store/user/$USER/test
    chirp eddie.crc.nd.edu:9094 rm /store/user/$USER/test/.__acl
    chirp eddie.crc.nd.edu:9094 rmdir /store/user/$USER/test

After this, the Chirp server can be added to the `input` and `output`
settings of the configuration, as done in the examples.

Running a separate Chirp server as user
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a file called `acl` with default access permissions in, e.g., your
home directory via (you will need a valid proxy for this!)::

    echo "globus:`voms-proxy-info -identity|sed 's/ /_/g'` rwlda" > ~/acl

Then do something akin to the following command::

    chirp_server --root=<some_directory> -A ~/acl -p <your_port>

where the default port is `9094`, but may be occupied, in which case it
should be best to linearly increment this port until you find a free one.
The `root` directory given to the Chirp server can be the desired stage-out
location, or any directory above it.

.. note::
   If you are using Chirp to stage out to a server that cannot handle a
   high load, limit the connections by adding ``-M 50`` to the arguments.

You should test Chirp from another machine::

    voms-proxy-init -voms cms -valid 192:00
    chirp_put <some_file> <your_server>:<your_port> spam

If this command fails with a permission issue, make sure you do not have
any `.__acl` files lingering around in your stageout directory::

    find <your_stageout_directory> -name .__acl -exec rm \{} \;

and try again.  Then add the following line to either the input or output
argument of the :class:`~lobster.se.StorageConfiguration`::

    "chirp://<your_server>:<your_port>/<your_stageout_directory_minus_chirp_root>"

Using a Hadoop backend
~~~~~~~~~~~~~~~~~~~~~~

Running `Chirp` with a direct connection to a Hadoop storage element may
increase performance.  Setting it up, which can end up quite complex, at
Notre Dame would look akin to the following::

    cd /var/tmp/
    cp -r /usr/lib/hadoop/ .
    cp /usr/lib64/libhdfs* hadoop/lib/
    env JAVA_HOME=/etc/alternatives/java_sdk/ HADOOP_HOME=$PWD/hadoop chirp_server \
            --root=hdfs://eddie.crc.nd.edu:19000/<your_stageout_directory_wo_leading_hadoop> \
            -A ~/acl -p <your_port>

It may be necessary to adjust memory setting of the Java VM with, e.g.,
the option ``LIBHDFS_OPTS=-Xmx100m``.

Changing configuration options
------------------------------

Lobster copies the initial configuration to its working directory as
`config.py`.  This configuration can be changed to modify the settings of
a running Lobster instance.  These changes will be propagated when the
configuration is re-read by the Lobster main-loop after saving the file.
This may take a few minutes for changes to have an effect, Lobster show
logging messages about changes in both the main log and `configure.log` in
the working directory.

Only attributes mentioned as modifiable in the documentation of each class
can be changed.

Lobster also provides a ``configure`` convenience command to edit the
configuration, which will launch an editor to edit the current
configuration.

.. note::
   The ``configure`` command uses the environment variable ``EDITOR`` to
   determine which editor to use, and uses `vi` as a default.
