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

"""Options for advanced Feast configuration.

The options pattern provides extensibility for advanced Kubernetes configurations
without polluting the main API. This follows the same callable pattern as
kubeflow.trainer.options and kubeflow.spark.types.options for SDK consistency.
"""

from dataclasses import dataclass

from kubeflow.feast.backends.base import RuntimeBackend


@dataclass
class Name:
    """Set a custom name for the FeatureStore deployment.

    If not provided, a name will be auto-generated with format: feast-{uuid8}

    The name must follow DNS-1123 subdomain rules:
    - Lowercase alphanumeric characters, '-', or '.'
    - Start and end with alphanumeric character
    - Maximum 253 characters

    Supported backends:
        - Kubernetes

    Args:
        name: Custom name for the FeatureStore. Must be a valid Kubernetes resource name.

    Example:
        ```python
        from kubeflow.feast import FeastClient
        from kubeflow.feast.types.options import Name

        client = FeastClient()
        info = client.create_store(
            feast_project="my_project",
            options=[Name("my-feature-store")]
        )
        ```

    Note:
        This option is extracted early in the backend flow before CRD building,
        unlike other options which modify the CRD after it's built.
    """

    name: str

    def __call__(self, feature_store_cr: dict, backend: RuntimeBackend) -> None:
        from kubeflow.feast.backends.kubernetes.backend import KubernetesBackend

        if not isinstance(backend, KubernetesBackend):
            raise ValueError(
                f"Name option is not compatible with {type(backend).__name__}. "
                f"Supported backends: KubernetesBackend"
            )

        feature_store_cr.setdefault("metadata", {})["name"] = self.name


@dataclass
class Labels:
    """Add Kubernetes labels to FeatureStore resources (.metadata.labels).

    Labels are key-value pairs attached to Kubernetes resources for organization,
    selection, and grouping.

    Supported backends:
        - Kubernetes

    Args:
        labels: Dictionary of label key-value pairs.

    Example:
        options = [Labels({"app": "feast", "team": "data-eng"})]
        info = client.create_store(feast_project="my_project", options=options)
    """

    labels: dict[str, str]

    def __call__(self, feature_store_cr: dict, backend: RuntimeBackend) -> None:
        from kubeflow.feast.backends.kubernetes.backend import KubernetesBackend

        if not isinstance(backend, KubernetesBackend):
            raise ValueError(
                f"Labels option is not compatible with {type(backend).__name__}. "
                f"Supported backends: KubernetesBackend"
            )

        metadata = feature_store_cr.setdefault("metadata", {})
        existing = metadata.get("labels", {}) or {}
        existing.update(self.labels)
        metadata["labels"] = existing


@dataclass
class Annotations:
    """Add Kubernetes annotations to FeatureStore resources (.metadata.annotations).

    Annotations store non-identifying metadata that can be used by tools,
    libraries, or for documentation purposes.

    Supported backends:
        - Kubernetes

    Args:
        annotations: Dictionary of annotation key-value pairs.

    Example:
        options = [Annotations({"description": "Production feature store"})]
        info = client.create_store(feast_project="my_project", options=options)
    """

    annotations: dict[str, str]

    def __call__(self, feature_store_cr: dict, backend: RuntimeBackend) -> None:
        from kubeflow.feast.backends.kubernetes.backend import KubernetesBackend

        if not isinstance(backend, KubernetesBackend):
            raise ValueError(
                f"Annotations option is not compatible with {type(backend).__name__}. "
                f"Supported backends: KubernetesBackend"
            )

        metadata = feature_store_cr.setdefault("metadata", {})
        existing = metadata.get("annotations", {}) or {}
        existing.update(self.annotations)
        metadata["annotations"] = existing


@dataclass
class CronJobSchedule:
    """Configure a CronJob for periodic materialization.

    The Feast operator can manage a CronJob that periodically runs
    ``feast apply`` and ``feast materialize-incremental``.

    Supported backends:
        - Kubernetes

    Args:
        schedule: Cron expression (e.g. ``"0 * * * *"`` for hourly).
        commands: Optional list of commands to run. Defaults to apply + materialize-incremental.

    Example:
        options = [CronJobSchedule("0 */6 * * *")]
        info = client.create_store(feast_project="my_project", options=options)
    """

    schedule: str
    commands: list[str] | None = None

    def __call__(self, feature_store_cr: dict, backend: RuntimeBackend) -> None:
        from kubeflow.feast.backends.kubernetes.backend import KubernetesBackend

        if not isinstance(backend, KubernetesBackend):
            raise ValueError(
                f"CronJobSchedule option is not compatible with {type(backend).__name__}. "
                f"Supported backends: KubernetesBackend"
            )

        cron_job: dict = {"schedule": self.schedule}
        if self.commands:
            cron_job["containerConfigs"] = {"commands": self.commands}
        feature_store_cr.setdefault("spec", {})["cronJob"] = cron_job


@dataclass
class Replicas:
    """Set the number of pod replicas for the FeatureStore deployment.

    Requires DB-backed persistence for online store, offline store, and registry.

    Supported backends:
        - Kubernetes

    Args:
        count: Number of desired pod replicas.

    Example:
        options = [Replicas(3)]
        info = client.create_store(feast_project="my_project", options=options)
    """

    count: int

    def __call__(self, feature_store_cr: dict, backend: RuntimeBackend) -> None:
        from kubeflow.feast.backends.kubernetes.backend import KubernetesBackend

        if not isinstance(backend, KubernetesBackend):
            raise ValueError(
                f"Replicas option is not compatible with {type(backend).__name__}. "
                f"Supported backends: KubernetesBackend"
            )

        feature_store_cr.setdefault("spec", {})["replicas"] = self.count
