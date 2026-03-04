# Copyright 2025 The Kubeflow Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""FeastClient for Kubeflow SDK."""

from __future__ import annotations

import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, Union

from kubeflow.common.types import KubernetesBackendConfig
from kubeflow.feast.backends.kubernetes import KubernetesBackend
from kubeflow.feast.types.types import (
    FeastProjectSource,
    FeastStoreInfo,
    OfflineStoreConfig,
    OnlineStoreConfig,
    RegistryConfig,
)

if TYPE_CHECKING:
    import pandas as pd

logger = logging.getLogger(__name__)


class FeastClient:
    """Client for managing Feast FeatureStore deployments on Kubernetes
    and interacting with the Feast feature store API.

    This client provides two main capabilities:

    1. **Infrastructure management**: Create, monitor, and delete FeatureStore
       deployments managed by the Feast operator on Kubernetes/OpenShift.

    2. **Feature store operations**: Apply feature definitions, materialize
       features, retrieve online/offline features, and push data - all through
       a unified interface that auto-configures the Feast SDK client from
       operator-managed deployments.

    Requires the feast package to be installed for feature store operations.
    Install it with::

        pip install 'kubeflow[feast]'

    Examples:
        Create a feature store and retrieve online features::

            from kubeflow.feast import FeastClient, FeastProjectSource

            client = FeastClient()

            # Deploy a feature store via the operator
            info = client.create_store(
                feast_project="my_project",
                project_source=FeastProjectSource(
                    git_url="https://github.com/org/features.git",
                    git_ref="main",
                ),
            )

            # Wait for it to be ready
            info = client.wait_for_store_ready(info.name)

            # Get an auto-configured Feast FeatureStore client
            fs = client.get_feature_store(info.name)

            # Retrieve online features
            features = fs.get_online_features(
                features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
                entity_rows=[{"driver_id": 1001}],
            ).to_dict()

        Connect to an existing Feast deployment::

            client = FeastClient()
            stores = client.list_stores()
            fs = client.get_feature_store(stores[0].name)
    """

    def __init__(self, backend_config: KubernetesBackendConfig | None = None):
        """Initialize FeastClient.

        Args:
            backend_config: Backend configuration. Currently only KubernetesBackendConfig
                is supported. Defaults to KubernetesBackendConfig.

        Raises:
            ValueError: Invalid backend configuration.
        """
        if backend_config is None:
            backend_config = KubernetesBackendConfig()

        if isinstance(backend_config, KubernetesBackendConfig):
            self.backend = KubernetesBackend(backend_config)
        else:
            raise ValueError(f"Invalid backend config: {type(backend_config)}")

    # ── Infrastructure management (Feast Operator CRD) ──

    def create_store(
        self,
        feast_project: str,
        project_source: FeastProjectSource | None = None,
        online_store: OnlineStoreConfig | None = None,
        offline_store: OfflineStoreConfig | None = None,
        registry: RegistryConfig | None = None,
        options: list | None = None,
        timeout: int = 300,
        wait: bool = True,
    ) -> FeastStoreInfo:
        """Create a FeatureStore deployment via the Feast operator.

        This creates a FeatureStore custom resource that the Feast operator
        will reconcile into a running Feast deployment with online store,
        offline store, and registry services.

        Args:
            feast_project: Feast project identifier (alphanumeric, underscores, hyphens).
            project_source: Source for the feature repo. Provide a git URL or
                use minimal init. If not specified, the operator uses its defaults.
            online_store: Online store service configuration.
            offline_store: Offline store service configuration.
            registry: Registry service configuration.
            options: Optional list of configuration options (Name, Labels, Annotations,
                CronJobSchedule, Replicas). Options can be imported from
                ``kubeflow.feast.types.options``.
            timeout: Timeout in seconds to wait for the store to become ready.
                Only used when ``wait=True``.
            wait: If True (default), block until the store is ready.

        Returns:
            FeastStoreInfo with deployment details.

        Raises:
            ValueError: Input arguments are invalid.
            TimeoutError: Timeout creating or waiting for FeatureStore.
            RuntimeError: Failed to create FeatureStore.

        Examples:
            Minimal creation with defaults::

                info = client.create_store(feast_project="my_project")

            With git source and online store config::

                from kubeflow.feast import FeastProjectSource, OnlineStoreConfig
                from kubeflow.feast.types.options import Name, CronJobSchedule

                info = client.create_store(
                    feast_project="prod_features",
                    project_source=FeastProjectSource(
                        git_url="https://github.com/org/feature-repo.git",
                        git_ref="main",
                    ),
                    online_store=OnlineStoreConfig(persistence_type="redis"),
                    options=[
                        Name("prod-feast"),
                        CronJobSchedule("0 * * * *"),
                    ],
                )
        """
        info = self.backend.create_store(
            feast_project=feast_project,
            project_source=project_source,
            online_store=online_store,
            offline_store=offline_store,
            registry=registry,
            options=options,
        )

        if wait:
            info = self.backend.wait_for_store_ready(info.name, timeout=timeout)

        return info

    def get_store(self, name: str) -> FeastStoreInfo:
        """Get information about a FeatureStore deployment.

        Args:
            name: Name of the FeatureStore.

        Returns:
            FeastStoreInfo with deployment details.

        Raises:
            TimeoutError: Timeout getting FeatureStore.
            RuntimeError: FeatureStore not found or request failed.
        """
        return self.backend.get_store(name)

    def list_stores(self) -> list[FeastStoreInfo]:
        """List all FeatureStore deployments in the configured namespace.

        Returns:
            List of FeastStoreInfo objects. Empty list if no stores exist.

        Raises:
            TimeoutError: Timeout listing FeatureStores.
            RuntimeError: Failed to list FeatureStores.
        """
        return self.backend.list_stores()

    def delete_store(self, name: str) -> None:
        """Delete a FeatureStore deployment.

        This deletes the FeatureStore custom resource and all associated
        Kubernetes resources managed by the operator.

        Args:
            name: Name of the FeatureStore.

        Raises:
            TimeoutError: Timeout deleting FeatureStore.
            RuntimeError: FeatureStore not found or deletion failed.
        """
        self.backend.delete_store(name)

    def wait_for_store_ready(
        self,
        name: str,
        timeout: int = 300,
        polling_interval: int = 5,
    ) -> FeastStoreInfo:
        """Wait for a FeatureStore to reach Ready state.

        Args:
            name: Name of the FeatureStore.
            timeout: Maximum wait time in seconds.
            polling_interval: Seconds between status checks.

        Returns:
            FeastStoreInfo when the store is ready.

        Raises:
            TimeoutError: Store did not become ready within timeout.
            RuntimeError: Store reached Failed state.
        """
        return self.backend.wait_for_store_ready(
            name=name,
            timeout=timeout,
            polling_interval=polling_interval,
        )

    def get_client_config(self, name: str) -> str:
        """Get the client feature_store.yaml for a FeatureStore deployment.

        The Feast operator provisions a ConfigMap containing the client
        configuration required to connect to the deployed Feast services.

        Args:
            name: Name of the FeatureStore.

        Returns:
            The feature_store.yaml content as a string.

        Raises:
            TimeoutError: Timeout reading config.
            RuntimeError: Config not found or store not ready.
        """
        return self.backend.get_client_config(name)

    # ── Feature store operations (Feast SDK wrapper) ──

    def get_feature_store(self, name: str) -> Any:
        """Get an auto-configured Feast FeatureStore client for an operator-managed deployment.

        Reads the client configuration from the operator-generated ConfigMap
        and returns a connected ``feast.FeatureStore`` instance.

        Args:
            name: Name of the FeatureStore deployment.

        Returns:
            A ``feast.FeatureStore`` instance configured to connect to the
            operator-managed Feast services.

        Raises:
            ImportError: If the feast package is not installed.
            TimeoutError: Timeout reading configuration.
            RuntimeError: Store not found or not ready.

        Example::

            fs = client.get_feature_store("my-feature-store")
            online_features = fs.get_online_features(
                features=["driver_stats:conv_rate"],
                entity_rows=[{"driver_id": 1001}],
            )
        """
        try:
            from feast import FeatureStore
            from feast.repo_config import load_repo_config
        except ImportError as e:
            raise ImportError(
                "feast is not installed. Install it with:\n\n"
                "  pip install 'kubeflow[feast]'\n"
            ) from e

        yaml_content = self.backend.get_client_config(name)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "feature_store.yaml"
            config_path.write_text(yaml_content)
            repo_config = load_repo_config(
                Path(tmpdir), config_path
            )

        return FeatureStore(config=repo_config)

    def apply(
        self,
        name: str,
        objects: Union[list, Any],
        objects_to_delete: list | None = None,
    ) -> None:
        """Apply feature definitions to an operator-managed FeatureStore.

        Registers entities, feature views, feature services, and other
        Feast objects in the registry of the specified FeatureStore deployment.

        Args:
            name: Name of the FeatureStore deployment.
            objects: A single Feast object or list of Feast objects to register.
                Supported types: Entity, FeatureView, OnDemandFeatureView,
                BatchFeatureView, StreamFeatureView, FeatureService, DataSource.
            objects_to_delete: Optional list of Feast objects to delete from the registry.

        Raises:
            ImportError: If the feast package is not installed.
            TimeoutError: Timeout connecting to the store.
            RuntimeError: Store not found or apply failed.

        Example::

            from feast import Entity, FeatureView, Field, FileSource
            from feast.types import Float32, Int64

            driver = Entity(name="driver", join_keys=["driver_id"])
            driver_stats = FeatureView(
                name="driver_stats",
                entities=[driver],
                schema=[Field(name="conv_rate", dtype=Float32)],
                source=FileSource(path="data/driver_stats.parquet"),
            )

            client.apply("my-feature-store", [driver, driver_stats])
        """
        fs = self.get_feature_store(name)
        fs.apply(objects, objects_to_delete=objects_to_delete)

    def materialize(
        self,
        name: str,
        start_date: datetime,
        end_date: datetime,
        feature_views: list[str] | None = None,
    ) -> None:
        """Materialize features from offline to online store.

        Loads feature data in the specified time range from the offline store
        into the online store for low-latency serving.

        Args:
            name: Name of the FeatureStore deployment.
            start_date: Start of the time range to materialize.
            end_date: End of the time range to materialize.
            feature_views: Optional list of feature view names. If not specified,
                all eligible feature views are materialized.

        Raises:
            ImportError: If the feast package is not installed.
            TimeoutError: Timeout connecting to the store.
            RuntimeError: Store not found or materialization failed.

        Example::

            from datetime import datetime, timedelta

            client.materialize(
                "my-feature-store",
                start_date=datetime(2025, 1, 1),
                end_date=datetime.utcnow() - timedelta(minutes=5),
            )
        """
        fs = self.get_feature_store(name)
        fs.materialize(
            start_date=start_date,
            end_date=end_date,
            feature_views=feature_views,
        )

    def materialize_incremental(
        self,
        name: str,
        end_date: datetime,
        feature_views: list[str] | None = None,
    ) -> None:
        """Incrementally materialize features from the last materialized point.

        Args:
            name: Name of the FeatureStore deployment.
            end_date: End of the time range to materialize.
            feature_views: Optional list of feature view names.

        Raises:
            ImportError: If the feast package is not installed.
            TimeoutError: Timeout connecting to the store.
            RuntimeError: Store not found or materialization failed.

        Example::

            from datetime import datetime

            client.materialize_incremental(
                "my-feature-store",
                end_date=datetime.utcnow(),
            )
        """
        fs = self.get_feature_store(name)
        fs.materialize_incremental(
            end_date=end_date,
            feature_views=feature_views,
        )

    def get_online_features(
        self,
        name: str,
        features: list[str],
        entity_rows: list[dict[str, Any]],
        full_feature_names: bool = False,
    ) -> Any:
        """Retrieve latest online feature values.

        Args:
            name: Name of the FeatureStore deployment.
            features: List of feature references in the format
                ``"feature_view:feature_name"``.
            entity_rows: List of entity key dictionaries.
            full_feature_names: If True, feature names are prefixed with the
                feature view name.

        Returns:
            An ``OnlineResponse`` object. Use ``.to_dict()`` or ``.to_df()``
            to access the feature values.

        Raises:
            ImportError: If the feast package is not installed.

        Example::

            response = client.get_online_features(
                "my-feature-store",
                features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
                entity_rows=[{"driver_id": 1001}, {"driver_id": 1002}],
            )
            print(response.to_dict())
        """
        fs = self.get_feature_store(name)
        return fs.get_online_features(
            features=features,
            entity_rows=entity_rows,
            full_feature_names=full_feature_names,
        )

    def get_historical_features(
        self,
        name: str,
        entity_df: pd.DataFrame,
        features: list[str],
        full_feature_names: bool = False,
    ) -> Any:
        """Retrieve historical feature values for training or batch scoring.

        Performs a point-in-time join of feature data against the entity
        dataframe.

        Args:
            name: Name of the FeatureStore deployment.
            entity_df: DataFrame with entity keys and an ``event_timestamp`` column.
            features: List of feature references in the format
                ``"feature_view:feature_name"``.
            full_feature_names: If True, feature names are prefixed with the
                feature view name.

        Returns:
            A ``RetrievalJob`` object. Use ``.to_df()`` or ``.to_arrow()``
            to retrieve the feature data.

        Raises:
            ImportError: If the feast package is not installed.

        Example::

            import pandas as pd
            from datetime import datetime

            entity_df = pd.DataFrame({
                "driver_id": [1001, 1002],
                "event_timestamp": [
                    datetime(2025, 4, 12, 10, 59),
                    datetime(2025, 4, 12, 8, 12),
                ],
            })

            job = client.get_historical_features(
                "my-feature-store",
                entity_df=entity_df,
                features=["driver_stats:conv_rate", "driver_stats:acc_rate"],
            )
            training_df = job.to_df()
        """
        fs = self.get_feature_store(name)
        return fs.get_historical_features(
            entity_df=entity_df,
            features=features,
            full_feature_names=full_feature_names,
        )

    def push(
        self,
        name: str,
        push_source_name: str,
        df: pd.DataFrame,
        to: str = "online",
    ) -> None:
        """Push features to a push source for real-time ingestion.

        This updates all feature views that have the specified push source
        as their stream source.

        Args:
            name: Name of the FeatureStore deployment.
            push_source_name: Name of the push source to push data to.
            df: DataFrame containing the feature data to push.
            to: Target store - ``"online"`` (default), ``"offline"``, or ``"online_and_offline"``.

        Raises:
            ImportError: If the feast package is not installed.

        Example::

            import pandas as pd
            from datetime import datetime

            df = pd.DataFrame({
                "driver_id": [1001],
                "conv_rate": [0.85],
                "event_timestamp": [datetime.utcnow()],
            })
            client.push("my-feature-store", "driver_push_source", df)
        """
        try:
            from feast.data_source import PushMode
        except ImportError as e:
            raise ImportError(
                "feast is not installed. Install it with:\n\n"
                "  pip install 'kubeflow[feast]'\n"
            ) from e

        mode_map = {
            "online": PushMode.ONLINE,
            "offline": PushMode.OFFLINE,
            "online_and_offline": PushMode.ONLINE_AND_OFFLINE,
        }
        push_mode = mode_map.get(to)
        if push_mode is None:
            raise ValueError(
                f"Invalid push mode '{to}'. Must be one of: {list(mode_map.keys())}"
            )

        fs = self.get_feature_store(name)
        fs.push(push_source_name=push_source_name, df=df, to=push_mode)
