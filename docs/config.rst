.. _config:

Configuration
=============

Lobster can be configured either in YAML or python, where the latter
provides more flexibility and features, while the former can be denser.

Using python
------------

The following is an example of a simple python configuration used to
process a single dataset:

.. literalinclude:: ../../examples/simple.py
   :language: python

Using YAML
----------

For historical reasons, Lobster can read YAML configuration files.  The
keys in the configuration correspond to the arguments of
:class:`~lobster.core.config.Config`, where spaces are automatically
converted to underscores.  A major difference is that the dataset keys are
mixed into :class:`~lobster.core.workflow.Workflow` keys.  Lobster will
automatically disentangle these.  Example:

.. literalinclude:: ../../examples/simple.yaml
   :language: yaml

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

