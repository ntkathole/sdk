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

"""Deploy a production-grade feature store with Redis, CronJob, and replicas.

Prerequisites:
    - Kubernetes cluster with the Feast operator installed
    - pip install 'kubeflow[feast]'

Usage:
    python feast_deploy_and_serve.py
"""

from kubeflow.feast import (
    FeastClient,
    FeastProjectSource,
    OfflineStoreConfig,
    OnlineStoreConfig,
    RegistryConfig,
)
from kubeflow.feast.types.options import (
    Annotations,
    CronJobSchedule,
    Labels,
    Name,
    Replicas,
)

client = FeastClient()

# Deploy a full-featured store with advanced options
print("Creating production FeatureStore...")
info = client.create_store(
    feast_project="production_features",
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
    options=[
        Name("prod-features"),
        Labels({"team": "ml-platform", "env": "production"}),
        Annotations({"description": "Production feature store for recommendation models"}),
        CronJobSchedule("0 */6 * * *"),  # Materialize every 6 hours
        Replicas(3),
    ],
    timeout=900,
)

print(f"FeatureStore '{info.name}' is {info.state}")
print(f"  Feast version: {info.feast_version}")
print(f"  Online store:  {info.service_hostnames.online_store}")
print(f"  Offline store: {info.service_hostnames.offline_store}")
print(f"  Registry:      {info.service_hostnames.registry}")

# Retrieve the auto-configured Feast client for advanced operations
fs = client.get_feature_store("prod-features")
print(f"\nFeast project: {fs.project}")
print(f"Feature views: {[fv.name for fv in fs.list_feature_views()]}")
print(f"Entities: {[e.name for e in fs.list_entities()]}")

# Serve online features
response = client.get_online_features(
    "prod-features",
    features=["user_features:age", "user_features:total_purchases"],
    entity_rows=[{"user_id": 42}, {"user_id": 99}],
)
print(f"\nOnline features: {response.to_dict()}")
