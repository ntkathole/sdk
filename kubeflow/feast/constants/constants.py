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

"""Constants for the Kubeflow Feast integration."""

# Feast Operator CRD identifiers
FEAST_GROUP = "feast.dev"
FEAST_VERSION = "v1"
FEAST_PLURAL = "featurestores"
FEAST_KIND = "FeatureStore"

# FeatureStore phases (from operator)
FEAST_READY = "Ready"
FEAST_PENDING = "Pending"
FEAST_FAILED = "Failed"

# Default ports
FEAST_ONLINE_STORE_PORT = 80
FEAST_OFFLINE_STORE_PORT = 80
FEAST_REGISTRY_PORT = 80

# Default feature_store.yaml config map key
FEAST_CLIENT_CONFIG_KEY = "feature_store.yaml"
