# Feast Feature Store Examples

Examples demonstrating the Kubeflow Feast integration for managing feature
store deployments and serving features.

## Prerequisites

- Kubernetes cluster with the [Feast operator](https://docs.feast.dev/reference/feast-operator) installed
- `pip install 'kubeflow[feast]'`

## Examples

| Example | Description |
| ------- | ----------- |
| [feast_quickstart.py](feast_quickstart.py) | Deploy a feature store, apply definitions, materialize, and retrieve features |
| [feast_deploy_and_serve.py](feast_deploy_and_serve.py) | Deploy with advanced options (Redis, CronJob, replicas) and serve online features |
| [feast_historical_features.py](feast_historical_features.py) | Retrieve historical features for model training |
