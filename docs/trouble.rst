Troubleshooting
===============

.. contents::

Common Problems
---------------

Installation
~~~~~~~~~~~~

Python crashes due to library mismatches
........................................

Make sure that all commands are issued in after executing ``cmsenv``.  Also
attempt to clear out `$HOME/.local` and re-install.

Matplotlib fails to install
...........................

When using older CMSSW releases, `matplotlib` may fail to install.  This
can be solved by installing `numpy` first with ``pip install numpy``.

Configuration
~~~~~~~~~~~~~

Finding the right settings for resources
........................................

When running Lobster, the resources should get adjusted automatically.  To
avoid `WorkQueue` retrying tasks too often to find optimal settings, they
can be gathered from an old project via::

    find /my/working/directory -name "*.summary"|grep successful > mysummaries
    resource_monitor_histograms -L mysummaries -j 32 /my/web/output/directory my_project_name

Where ``-j 32`` should be adjusted to match the cores of the machine.
The output directory `/my/web/output/directory` then contains a file
`stats.json` with the resources that `WorkQueue` deems optimal, and
`index.html` has a tabular representation.

Mixing DBS and local datasets
.............................

Mixing datasets with inputs accessed via AAA and custom input configuration
is currently not supported.

Example Hacks
---------------

Configuration
~~~~~~~~~~~~~

Changing an immutable attribute in the pickled configuration
............................................................

You should never need to do this. You should only change mutable configuration
attributes using the ``configure`` command via `lobster configure config.py`.
The point of the `PartiallyMutable` metaclass is to restrict configuration
options from changing unless they have been declared mutable and a callback
function has been defined indicating how Lobster should deal with the change.
Unexpected things can happen otherwise. First ask yourself why you're doing
this before how to do it. If you're still determined, here's an example which
changes the `label` attribute::

    import datetime
    import os
    import shutil

    from lobster.core import config
    from lobster import util

    wdir = "/path/to/working/directory"
    shutil.copy(
        os.path.join(wdir, "config.pkl"),
        os.path.join(wdir, "config.pkl.{:%Y-%m-%d_%H%M%S}".format(datetime.datetime.now())))

    cfg = config.Config.load("/path/to/working/directory")
    with util.PartiallyMutable.unlock():
        cfg.label = 'foo'
    cfg.save()
