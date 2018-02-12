.. Lobster documentation master file, created by
   sphinx-quickstart on Thu Oct 29 09:06:49 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Lobster
=======

A userspace workflow management tool for harnessing non-dedicated resources
for high-throughput workloads.

CMS lacks tools for users to run their jobs at sites not conforming to WLCG
standards.  Lobster aims to alleviate this problem by providing a workflow
management to run jobs depending on CMSSW that does not have any specific
requirements to the cluster or need for administrative privileges.

Features
--------

* Access to more compute nodes
* No special access privileges or setup required
* Dynamic task handling, handling eviction
* Live and historical view of system properties

.. toctree::
   :caption: Overview
   :maxdepth: 1

   intro
   monitor

.. toctree::
   :caption: Documentation
   :maxdepth: 2

   install
   config
   run
   trouble

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

.. _github: https://github.com/matz-e/lobster
