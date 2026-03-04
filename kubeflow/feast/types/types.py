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

"""Types for Kubeflow Feast SDK."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class FeastStoreState(str, Enum):
    """State of a FeatureStore deployment managed by the Feast operator."""

    READY = "Ready"
    PENDING = "Pending"
    FAILED = "Failed"


@dataclass
class ServiceHostnames:
    """Service hostnames for a FeatureStore deployment.

    Hostnames are in the format ``<service>.<namespace>.svc.cluster.local:<port>``.

    Args:
        online_store: Hostname for the online store (feature server).
        offline_store: Hostname for the offline store server.
        registry: Hostname for the registry gRPC server.
        registry_rest: Hostname for the registry REST API server.
        ui: Hostname for the Feast UI.
    """

    online_store: str | None = None
    offline_store: str | None = None
    registry: str | None = None
    registry_rest: str | None = None
    ui: str | None = None


@dataclass
class FeastStoreInfo:
    """Information about a FeatureStore deployment managed by the Feast operator.

    Args:
        name: Name of the FeatureStore custom resource.
        namespace: Kubernetes namespace.
        state: Current deployment phase (Ready, Pending, Failed).
        feast_project: The Feast project identifier.
        feast_version: Version of Feast deployed by the operator.
        client_config_map: Name of the ConfigMap containing the client feature_store.yaml.
        service_hostnames: Hostnames for the deployed Feast services.
        creation_timestamp: Timestamp when the FeatureStore was created.
    """

    name: str
    namespace: str
    state: FeastStoreState
    feast_project: str | None = None
    feast_version: str | None = None
    client_config_map: str | None = None
    service_hostnames: ServiceHostnames = field(default_factory=ServiceHostnames)
    creation_timestamp: datetime | None = None


@dataclass
class OnlineStoreConfig:
    """Configuration for the online store service.

    Args:
        persistence_type: Type of online store persistence (e.g. "sqlite", "redis", "postgres").
        resources: Kubernetes resource requirements as dict (e.g. {"cpu": "1", "memory": "2Gi"}).
        image: Custom container image for the online store server.
        replicas: Number of replicas.
    """

    persistence_type: str | None = None
    resources: dict[str, str] | None = None
    image: str | None = None
    replicas: int | None = None


@dataclass
class OfflineStoreConfig:
    """Configuration for the offline store service.

    Args:
        persistence_type: Type of offline store persistence (e.g. "dask", "duckdb").
        resources: Kubernetes resource requirements as dict.
        image: Custom container image for the offline store server.
    """

    persistence_type: str | None = None
    resources: dict[str, str] | None = None
    image: str | None = None


@dataclass
class RegistryConfig:
    """Configuration for the registry service.

    Args:
        registry_type: Type of registry ("local" or "remote").
        persistence_type: Persistence type for local registries (e.g. "file", "sql").
        resources: Kubernetes resource requirements as dict.
        image: Custom container image for the registry server.
    """

    registry_type: str | None = None
    persistence_type: str | None = None
    resources: dict[str, str] | None = None
    image: str | None = None


@dataclass
class FeastProjectSource:
    """Source configuration for the Feast project directory.

    Either ``git_url`` (with optional ``git_ref``) or ``minimal_init`` should be provided.

    Args:
        git_url: Git repository URL containing the feature repo.
        git_ref: Git branch, tag, or commit reference.
        feature_repo_path: Relative path to the feature repo within the git repository.
        minimal_init: If True, run ``feast init --minimal`` instead of cloning a repo.
    """

    git_url: str | None = None
    git_ref: str | None = None
    feature_repo_path: str | None = None
    minimal_init: bool = False
