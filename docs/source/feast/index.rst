Feast Feature Store
===================

Manage feature store deployments and serve features for machine learning.

Overview
--------

The Kubeflow Feast integration provides a unified client for managing
`Feast <https://feast.dev/>`_ feature store deployments on Kubernetes/OpenShift
and interacting with the Feast feature store API.

The ``FeastClient`` offers two capabilities:

- **Infrastructure management** — Create, monitor, and delete FeatureStore
  deployments managed by the `Feast operator <https://github.com/feast-dev/feast/tree/master/infra/feast-operator>`_
  on Kubernetes.
- **Feature store operations** — Apply feature definitions, materialize features,
  and retrieve online/offline features through an auto-configured Feast SDK client.

Installation
------------

Install Feast support as an optional dependency:

.. code-block:: bash

   pip install 'kubeflow[feast]'

This installs the ``feast`` Python package alongside the Kubeflow SDK.

.. note::

   The Feast operator must be installed on your Kubernetes cluster to manage
   FeatureStore deployments. See the
   `Feast operator documentation <https://docs.feast.dev/reference/feast-operator>`_
   for installation instructions.

Quick Example
-------------

.. code-block:: python

   from kubeflow.feast import FeastClient, FeastProjectSource, OnlineStoreConfig
   from kubeflow.feast.types.options import Name, CronJobSchedule

   client = FeastClient()

   # Deploy a feature store via the Feast operator
   info = client.create_store(
       feast_project="my_project",
       project_source=FeastProjectSource(
           git_url="https://github.com/org/feature-repo.git",
           git_ref="main",
       ),
       online_store=OnlineStoreConfig(persistence_type="redis"),
       options=[
           Name("prod-feast"),
           CronJobSchedule("0 * * * *"),  # Hourly materialization
       ],
   )

   # Retrieve online features
   response = client.get_online_features(
       "prod-feast",
       features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
       entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
   )
   print(response.to_dict())

How It Works
------------

1. **Deploy** — ``create_store()`` creates a ``FeatureStore`` custom resource that the
   Feast operator reconciles into online store, offline store, and registry services.
2. **Connect** — ``get_feature_store()`` reads the operator-generated client configuration
   from a Kubernetes ConfigMap and returns a fully configured ``feast.FeatureStore`` instance.
3. **Operate** — Use convenience methods (``apply``, ``materialize``, ``get_online_features``,
   ``get_historical_features``, ``push``) or work directly with the Feast ``FeatureStore`` object.

Key Concepts
------------

**FeatureStore Deployment**
   A Kubernetes custom resource (``feast.dev/v1 FeatureStore``) that the Feast operator
   reconciles into running services. Includes online store, offline store, registry,
   and optional UI and CronJob components.

**Feature View**
   A group of related features derived from a data source, defined in the feature
   repository. Feature views are registered via ``apply()`` and served from the
   online or offline store.

**Materialization**
   The process of loading feature data from the offline store (batch source) into
   the online store for low-latency serving. Can be triggered manually or via a
   CronJob managed by the operator.

**Online Features**
   Latest feature values served from the online store with low latency, used for
   real-time inference.

**Historical Features**
   Point-in-time correct feature values retrieved from the offline store, used for
   training and batch scoring.

Infrastructure Management
-------------------------

Create a FeatureStore deployment:

.. code-block:: python

   from kubeflow.feast import (
       FeastClient,
       FeastProjectSource,
       OnlineStoreConfig,
       OfflineStoreConfig,
       RegistryConfig,
   )

   client = FeastClient()

   info = client.create_store(
       feast_project="production",
       project_source=FeastProjectSource(
           git_url="https://github.com/org/feature-repo.git",
           git_ref="main",
           feature_repo_path="feature_repo",
       ),
       online_store=OnlineStoreConfig(
           persistence_type="redis",
           resources={"cpu": "2", "memory": "4Gi"},
       ),
       offline_store=OfflineStoreConfig(persistence_type="duckdb"),
       registry=RegistryConfig(
           registry_type="local",
           persistence_type="sql",
       ),
   )

List and inspect deployments:

.. code-block:: python

   # List all FeatureStore deployments
   for store in client.list_stores():
       print(f"{store.name}: {store.state} (project: {store.feast_project})")

   # Get detailed info
   info = client.get_store("prod-feast")
   print(f"Online store: {info.service_hostnames.online_store}")
   print(f"Registry: {info.service_hostnames.registry}")

Delete a deployment:

.. code-block:: python

   client.delete_store("prod-feast")

Feature Store Operations
------------------------

**Apply feature definitions:**

.. code-block:: python

   from feast import Entity, FeatureView, Field, FileSource
   from feast.types import Float32, Int64

   driver = Entity(name="driver", join_keys=["driver_id"])

   driver_stats = FeatureView(
       name="driver_stats",
       entities=[driver],
       schema=[
           Field(name="conv_rate", dtype=Float32),
           Field(name="acc_rate", dtype=Float32),
       ],
       source=FileSource(path="data/driver_stats.parquet"),
   )

   client.apply("prod-feast", [driver, driver_stats])

**Materialize features:**

.. code-block:: python

   from datetime import datetime, timedelta

   # Full materialization
   client.materialize(
       "prod-feast",
       start_date=datetime(2025, 1, 1),
       end_date=datetime.utcnow() - timedelta(minutes=5),
   )

   # Incremental materialization (from last checkpoint)
   client.materialize_incremental(
       "prod-feast",
       end_date=datetime.utcnow(),
   )

**Retrieve online features:**

.. code-block:: python

   response = client.get_online_features(
       "prod-feast",
       features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
       entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
   )
   print(response.to_dict())

**Retrieve historical features for training:**

.. code-block:: python

   import pandas as pd
   from datetime import datetime

   entity_df = pd.DataFrame({
       "driver_id": [1001, 1002, 1003],
       "event_timestamp": [
           datetime(2025, 4, 12, 10, 59),
           datetime(2025, 4, 12, 8, 12),
           datetime(2025, 4, 11, 16, 30),
       ],
   })

   job = client.get_historical_features(
       "prod-feast",
       entity_df=entity_df,
       features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
   )
   training_df = job.to_df()

**Push real-time features:**

.. code-block:: python

   import pandas as pd
   from datetime import datetime

   df = pd.DataFrame({
       "driver_id": [1001],
       "conv_rate": [0.85],
       "event_timestamp": [datetime.utcnow()],
   })
   client.push("prod-feast", "driver_push_source", df)

Advanced: Direct Feast FeatureStore Access
------------------------------------------

For operations not covered by the convenience methods, obtain an auto-configured
``feast.FeatureStore`` client:

.. code-block:: python

   fs = client.get_feature_store("prod-feast")

   # Use any Feast API directly
   fs.list_feature_views()
   fs.list_entities()
   fs.refresh_registry()

Options
-------

Options follow the SDK-wide callable pattern used by Trainer and Spark clients.

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Option
     - Description
   * - ``Name(name)``
     - Set a custom name for the FeatureStore (default: auto-generated ``feast-{uuid8}``).
   * - ``Labels(labels)``
     - Add Kubernetes labels to the FeatureStore resource.
   * - ``Annotations(annotations)``
     - Add Kubernetes annotations to the FeatureStore resource.
   * - ``CronJobSchedule(schedule, commands=None)``
     - Configure periodic materialization via a Kubernetes CronJob.
   * - ``Replicas(count)``
     - Set the number of pod replicas (requires DB-backed persistence).

.. code-block:: python

   from kubeflow.feast.types.options import (
       Name, Labels, Annotations, CronJobSchedule, Replicas,
   )

   info = client.create_store(
       feast_project="my_project",
       options=[
           Name("prod-feast"),
           Labels({"team": "ml-platform", "env": "production"}),
           Annotations({"description": "Production feature store"}),
           CronJobSchedule("0 */6 * * *"),  # Every 6 hours
           Replicas(3),
       ],
   )
