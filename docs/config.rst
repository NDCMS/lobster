.. _config:

Configuration
=============

Example configurations
----------------------

The following is an example of a simple python configuration used to
process a single dataset:

.. literalinclude:: ../examples/simple.py
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

