Introduction
============

`Lobster` was built to allow users to run CMS and other workflows on
non-dedicated resources without any special access privileges.
As the computing tasks of a user may be terminated without any warning, or
evicted, `Lobster` focuses on processing small units of work at a time.

To facilitate this, `Lobster` is set up using a central server or master
for each project, launched as a user-specific daemon, that connects to
workers running on the resources available.
The communication and distribution of work happens via `WorkQueue`_, which
also allows the user to launch workers easily via a `factory`_.

.. figure:: diagrams/overview.png
   :figwidth: 80%
   :align: center
   :alt: Conceptual overview of Lobster

   Running `Lobster`: the user launches the central `Lobster` process and
   submits workers to the clusters via a `factory`_.
   To process the data, small tasks are created and handed of to
   `WorkQueue`_ for distribution to the workers.
   The state of each unit of work is tracked in a database and reassigned
   to newly created tasks in case a previous attempt to process it failed.

The `Lobster` master creates small tasks which are distributed to the
workers.
If the required software environment is not present on the machine a worker
runs on, the task wrapper re-runs itself within a `Parrot`_ instance that
can provide software via a `CVMFS` mount-point overlaid over the file-system.
Runtime files and data accessed through `Parrot`_ are cached by the worker
and shared between tasks running in parallel and sequence.

.. figure:: diagrams/worker.png
   :figwidth: 80%
   :align: center
   :alt: Conceptual overview of a worker

   Execution of tasks on a worker: every worker can execute several tasks
   in parallel, if the resource allotment allows to do so.
   If needed, a `Parrot`_ instance is launched for each task to provide the
   software environment.
   To speed up execution, software and runtime data are downloaded only
   once and stored in a worker-specific cache to be shared by the all
   tasks.

The resources that every task consumes are monitored and used to determine
how to best distribute tasks, optimizing the overall system usage.
`Lobster` compiles the monitoring information in regular intervals to give
an overview of the data processing progress and performance.

.. _WorkQueue: http://ccl.cse.nd.edu/software/workqueue/
.. _factory: http://ccl.cse.nd.edu/software/manuals/man/work_queue_factory.html
.. _Parrot: http://ccl.cse.nd.edu/software/parrot/

Further Information
-------------------

More details on the implementation and scope of `Lobster` or the underlying
concept of running CMS workflows via `Parrot`_ can be found in the
following conference proceedings:

.. bibliography:: papers.bib
   :style: custom
   :list: bullet
   :all:

Additional context may also be provided by selected talks:

.. only:: builder_html

   * :download:`Benjamin Tovar, HTCondor week <talks/2015-htcondor-tovar.pdf>`, May 2015
   * :download:`Douglas Thain, CVMFS Workshop <talks/2015-cvmfs-thain.pdf>`, March 2015
   * :download:`Kevin Lannon, internal USCMS computing workshop <talks/2015-uscms-lannon.pdf>`, February 2015
   * :download:`Matthias Wolf, internal OSG-connect meeting <talks/2014-osg-connect-wolf.pdf>`, June 2014
