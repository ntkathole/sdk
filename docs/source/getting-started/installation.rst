Installation
============

Requirements
------------

- Python 3.10 or higher
- Access to a Kubernetes cluster (for Kubernetes backend)

Install from PyPI
-----------------

.. code-block:: bash

   pip install kubeflow

Optional Dependencies
---------------------

For Docker container backend:

.. code-block:: bash

   pip install kubeflow[docker]

For Podman container backend:

.. code-block:: bash

   pip install kubeflow[podman]

For Feast feature store:

.. code-block:: bash

   pip install 'kubeflow[feast]'

For Model Registry:

.. code-block:: bash

   pip install 'kubeflow[hub]'

For Spark:

.. code-block:: bash

   pip install 'kubeflow[spark]'

Install from Source
-------------------

.. code-block:: bash

   git clone https://github.com/kubeflow/sdk.git
   cd sdk
   pip install -e .

Verify Installation
-------------------

.. code-block:: python

   import kubeflow
   print(kubeflow.__version__)

Next Steps
----------

Continue to :doc:`quickstart` to run your first training job.
