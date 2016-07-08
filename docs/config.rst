.. _config:

Configuration
=============

Example configurations
----------------------

All configurations in this section are also available in the `examples`
directory of the Lobster source.

Simple dataset processing
~~~~~~~~~~~~~~~~~~~~~~~~~

The following is an example of a simple python configuration used to
process a single dataset:

.. literalinclude:: ../examples/simple.py
   :language: python

MC generation à la production
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Lobster has the ability to reproduce the production workflows used in CMS
for Monte-Carlo production.  As a first step, the steps of a workflow have
to be downloaded and the release areas prepared:

.. literalinclude:: ../examples/mc_setup.sh
   :language: shell

The setup created by this script can be run by the following Lobster
configuration, which sports a workflow for each step of the official MC
production chain:

.. literalinclude:: ../examples/mc_gen.py
   :language: python

The configuration components
----------------------------

General Options
~~~~~~~~~~~~~~~

.. autoclass:: lobster.core.config.Config

.. autoclass:: lobster.core.config.AdvancedOptions

.. autoclass:: lobster.se.StorageConfiguration

Workflow specification
~~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: lobster.core.workflow.Category

.. autoclass:: lobster.core.workflow.Workflow

Dataset specification
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: lobster.core.dataset.Dataset

.. autoclass:: lobster.core.dataset.ParentDataset

.. autoclass:: lobster.core.dataset.ProductionDataset

.. autoclass:: lobster.cmssw.dataset.Dataset

Monitoring
~~~~~~~~~~

.. autoclass:: lobster.monitor.elk.ElkInterface

.. note::
     At Notre Dame, Elasticsearch is accessible at ``elk.crc.nd.edu:9200`` and Kibana is accessible at ``elk.crc.nd.edu:5601``
