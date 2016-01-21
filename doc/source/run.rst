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
commands 

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

.. _submission:
Submitting workers
------------------

Workers can be either submitted directly, as shown above, or by using the
`WorkQueue` factory, which allows for dynamic scaling of the processing.
The factory uses a config written in JSON, like:

.. code-block:: json

    {
      "master-name": "lobster_shuffle.*",
      "max-workers": 100,
      "min-workers": 0,
      "cores": 4,
      "memory": 3600,
      "disk": 12000
    }

which sets up 4 core workers providing 3.6 GB of RAM and 12 GB of disk
space.  The factory can then be started with::

    work_queue_factory -T condor -C config.json

Using a Chirp server
--------------------
