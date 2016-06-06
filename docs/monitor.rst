Monitoring
==========

`Lobster` produces monitoring plots, which are saved into a directory
either as specified in the :ref:`configuring Lobster <config>`, or by
issuing the following command::

    lobster plot --outdir <monitoring directory> <configuration>

The monitoring is split into a `Lobster` overview page and per-category
pages displaying progress and task status.

Task Exit Codes
---------------

`Lobster` uses the following error codes, which are referred to in the
`Failed Tasks` section of the category monitoring pages:

===== ======
Code  Reason
===== ======
169   Unable to run parrot
170   Sandbox unpacking failure
171   Failed to determine base release
172   Failed to find old releasetop
173   Failed to create new release area
174   `cmsenv` failure
179   Stagein failure
180   Prologue failure
185   Failed to run command
190   Failed to parse report.xml
191   Failed to parse wrapper timing information
199   Epilogue failure
200   Generic parrot failure
210   Stageout failure during transfer
211   Stageout failure cross-checking transfer
500   Publish failure
10001 Generic task failure reported by `WorkQueue`
10010 Task timed out
10020 Task exceeded maximum number of retries
10030 Task exceeded maximum runtime
10040 Task exceeded maximum memory
10050 Task exceeded maximum disk
===== ======

Error codes lower than 170 may indicate a ``cmsRun`` problem, codes
O(1k) may hint at a `CMS configuration or runtime problem`_.
Codes O(10k) are internal Work Queue error codes and may be bitmasked
together, i.e., 100514 is a combination of errors 100512 and 100002.

.. _CMS configuration or runtime problem: https://twiki.cern.ch/twiki/bin/view/CMSPublic/JobExitCodes
