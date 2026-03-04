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

"""Feast quickstart: deploy a feature store and retrieve online features.

Prerequisites:
    - Kubernetes cluster with the Feast operator installed
    - pip install 'kubeflow[feast]'

Usage:
    python feast_quickstart.py
"""

from kubeflow.feast import FeastClient, FeastProjectSource
from kubeflow.feast.types.options import Name

client = FeastClient()

# Step 1: Deploy a feature store from a git-hosted feature repo
print("Creating FeatureStore deployment...")
info = client.create_store(
    feast_project="driver_ranking",
    project_source=FeastProjectSource(
        git_url="https://github.com/feast-dev/feast-driver-ranking-tutorial.git",
        git_ref="main",
    ),
    options=[Name("driver-ranking")],
    timeout=600,
)
print(f"FeatureStore '{info.name}' is {info.state}")
print(f"  Online store: {info.service_hostnames.online_store}")
print(f"  Registry:     {info.service_hostnames.registry}")

# Step 2: Retrieve online features
print("\nRetrieving online features...")
response = client.get_online_features(
    "driver-ranking",
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
    entity_rows=[
        {"driver_id": 1001},
        {"driver_id": 1002},
        {"driver_id": 1003},
    ],
)
print(response.to_dict())

# Step 3: List all feature stores
print("\nAll FeatureStore deployments:")
for store in client.list_stores():
    print(f"  {store.name}: {store.state} (project: {store.feast_project})")

# Step 4: Clean up
# client.delete_store("driver-ranking")
