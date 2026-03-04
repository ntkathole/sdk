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

"""Public API for the Kubeflow Feast client and types. Import from kubeflow.feast."""

from kubeflow.common.types import KubernetesBackendConfig
from kubeflow.feast.api.feast_client import FeastClient
from kubeflow.feast.types.options import (
    Annotations,
    CronJobSchedule,
    Labels,
    Name,
    Replicas,
)
from kubeflow.feast.types.types import (
    FeastProjectSource,
    FeastStoreInfo,
    FeastStoreState,
    OfflineStoreConfig,
    OnlineStoreConfig,
    RegistryConfig,
    ServiceHostnames,
)

__all__ = [
    # Core API
    "FeastClient",
    # Types
    "FeastProjectSource",
    "FeastStoreInfo",
    "FeastStoreState",
    "OfflineStoreConfig",
    "OnlineStoreConfig",
    "RegistryConfig",
    "ServiceHostnames",
    # Options (callable pattern like trainer/spark SDKs)
    "Annotations",
    "CronJobSchedule",
    "Labels",
    "Name",
    "Replicas",
    # Configuration
    "KubernetesBackendConfig",
]
