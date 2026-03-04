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

"""Retrieve historical features for model training.

This example shows how to use the Kubeflow Feast client to perform a
point-in-time join for generating training datasets.

Prerequisites:
    - Kubernetes cluster with the Feast operator installed
    - An existing FeatureStore deployment named "prod-feast"
    - pip install 'kubeflow[feast]'

Usage:
    python feast_historical_features.py
"""

from datetime import datetime, timedelta

import pandas as pd

from kubeflow.feast import FeastClient

client = FeastClient()

# Materialize recent data into the online store
print("Materializing features...")
client.materialize_incremental(
    "prod-feast",
    end_date=datetime.utcnow() - timedelta(minutes=5),
)

# Build an entity dataframe for training
entity_df = pd.DataFrame(
    {
        "driver_id": [1001, 1002, 1003, 1004, 1005],
        "event_timestamp": [
            datetime(2025, 6, 1, 10, 0),
            datetime(2025, 6, 1, 11, 30),
            datetime(2025, 5, 28, 8, 15),
            datetime(2025, 5, 30, 14, 45),
            datetime(2025, 6, 1, 9, 0),
        ],
    }
)

# Retrieve historical features with point-in-time correctness
print("Retrieving historical features...")
job = client.get_historical_features(
    "prod-feast",
    entity_df=entity_df,
    features=[
        "driver_hourly_stats:conv_rate",
        "driver_hourly_stats:acc_rate",
        "driver_hourly_stats:avg_daily_trips",
    ],
)

training_df = job.to_df()
print(f"\nTraining dataset shape: {training_df.shape}")
print(training_df.head())

# Push real-time features
print("\nPushing real-time features...")
push_df = pd.DataFrame(
    {
        "driver_id": [1001],
        "conv_rate": [0.92],
        "acc_rate": [0.78],
        "avg_daily_trips": [15],
        "event_timestamp": [datetime.utcnow()],
    }
)
client.push("prod-feast", "driver_stats_push_source", push_df)
print("Push complete.")
