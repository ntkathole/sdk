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

"""Base class for Feast backends."""

import abc

from kubeflow.feast.types.types import (
    FeastProjectSource,
    FeastStoreInfo,
    OfflineStoreConfig,
    OnlineStoreConfig,
    RegistryConfig,
)


class RuntimeBackend(abc.ABC):
    """Abstract base class for Feast backends.

    All Feast backends must implement these methods to manage FeatureStore deployments.
    """

    @abc.abstractmethod
    def create_store(
        self,
        feast_project: str,
        project_source: FeastProjectSource | None = None,
        online_store: OnlineStoreConfig | None = None,
        offline_store: OfflineStoreConfig | None = None,
        registry: RegistryConfig | None = None,
        options: list | None = None,
    ) -> FeastStoreInfo:
        """Create a new FeatureStore deployment.

        Args:
            feast_project: Feast project identifier.
            project_source: Source for the feature repo (git or init).
            online_store: Online store configuration.
            offline_store: Offline store configuration.
            registry: Registry configuration.
            options: List of configuration options (use Name option for custom name).

        Returns:
            FeastStoreInfo with deployment details (may be in PENDING state).

        Raises:
            TimeoutError: If the creation request times out.
            RuntimeError: If store creation fails.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_store(self, name: str) -> FeastStoreInfo:
        """Get information about a FeatureStore deployment.

        Args:
            name: FeatureStore name.

        Returns:
            FeastStoreInfo with deployment details.

        Raises:
            TimeoutError: If the request times out.
            RuntimeError: If the store is not found or request fails.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def list_stores(self) -> list[FeastStoreInfo]:
        """List all FeatureStore deployments.

        Returns:
            List of FeastStoreInfo objects.

        Raises:
            TimeoutError: If the request times out.
            RuntimeError: If listing fails.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_store(self, name: str) -> None:
        """Delete a FeatureStore deployment.

        Args:
            name: FeatureStore name.

        Raises:
            TimeoutError: If the deletion request times out.
            RuntimeError: If the store is not found or deletion fails.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def wait_for_store_ready(
        self,
        name: str,
        timeout: int = 300,
        polling_interval: int = 5,
    ) -> FeastStoreInfo:
        """Wait for a FeatureStore deployment to become ready.

        Polls the store status until it reaches Ready state or times out.

        Args:
            name: FeatureStore name.
            timeout: Maximum wait time in seconds. Default 300 (5 minutes).
            polling_interval: Seconds between status checks. Default 5.

        Returns:
            FeastStoreInfo when the store is ready.

        Raises:
            TimeoutError: If store does not become ready within timeout.
            RuntimeError: If store fails.
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def get_client_config(self, name: str) -> str:
        """Get the client feature_store.yaml content for a FeatureStore deployment.

        The Feast operator creates a ConfigMap with the client configuration
        that can be used to connect to the deployed Feast services.

        Args:
            name: FeatureStore name.

        Returns:
            The feature_store.yaml content as a string.

        Raises:
            TimeoutError: If the request times out.
            RuntimeError: If the config is not found or request fails.
        """
        raise NotImplementedError()
