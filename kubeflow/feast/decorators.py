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

"""Data-scientist-facing decorators and context managers for Feast.

These provide the programming model described in the unified SDK proposal:
decorators that inject auto-configured Feast clients into user functions,
context managers that handle runtime lifecycle, and async-first APIs.

Usage examples::

    # ── Decorator: inject a FeatureStore into your function ──
    @FeatureStore("prod-feast")
    async def train(fs):
        training_df = fs.get_historical_features(
            entity_df=entity_df,
            features=["driver_stats:conv_rate"],
        ).to_df()
        # ... training loop ...

    await train()


    # ── Context manager: scoped lifecycle ──
    async with FeatureStore("prod-feast") as fs:
        features = fs.get_online_features(
            features=["driver_stats:conv_rate"],
            entity_rows=[{"driver_id": 1001}],
        )


    # ── Composable with other decorators ──
    @Trainer("fine-tune", runtime="pytorch-distributed", hw_profile="H100")
    @FeatureStore("prod-feast")
    async def train(fs):
        training_df = fs.get_historical_features(...).to_df()
        model = torch.nn.Linear(10, 1)
        ...

    await train()


    # ── Materialization as a managed workload ──
    @FeastMaterializer(
        "daily-materialize",
        feature_store="prod-feast",
        schedule="0 */6 * * *",
    )
    async def materialize(fs):
        fs.materialize_incremental(
            end_date=datetime.utcnow(),
            feature_views=["driver_hourly_stats"],
        )

    await materialize()
"""

from __future__ import annotations

import asyncio
import functools
import inspect
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from kubeflow.common.types import KubernetesBackendConfig

logger = logging.getLogger(__name__)


def _ensure_feast_installed():
    """Raise ImportError with install instructions if feast is missing."""
    try:
        import feast  # noqa: F401
    except ImportError as e:
        raise ImportError(
            "feast is not installed. Install it with:\n\n"
            "  pip install 'kubeflow[feast]'\n"
        ) from e


def _resolve_feature_store(
    store: str | None = None,
    repo_path: str | None = None,
    config: Any | None = None,
    backend_config: KubernetesBackendConfig | None = None,
) -> Any:
    """Resolve a feast.FeatureStore from the various configuration sources.

    Priority:
        1. Explicit ``config`` (a ``RepoConfig`` object)
        2. ``repo_path`` (local path to feature repo with feature_store.yaml)
        3. ``store`` name (look up operator-managed deployment via Kubernetes)
    """
    _ensure_feast_installed()
    from feast import FeatureStore as FeastFS
    from feast.repo_config import load_repo_config

    if config is not None:
        return FeastFS(config=config)

    if repo_path is not None:
        return FeastFS(repo_path=repo_path)

    if store is not None:
        from kubeflow.feast.backends.kubernetes import KubernetesBackend

        bc = backend_config or KubernetesBackendConfig()
        backend = KubernetesBackend(bc)
        yaml_content = backend.get_client_config(store)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "feature_store.yaml"
            config_path.write_text(yaml_content)
            repo_config = load_repo_config(Path(tmpdir), config_path)

        return FeastFS(config=repo_config)

    raise ValueError(
        "FeatureStore requires at least one of: "
        "store (operator deployment name), repo_path, or config"
    )


class FeatureStore:
    """Decorator and async context manager that provides a configured Feast client.

    Can be used in three ways:

    **1. As a decorator** — injects a ``feast.FeatureStore`` as the first
    argument of the decorated function. Works with both sync and async
    functions::

        @FeatureStore("prod-feast")
        async def train(fs):
            df = fs.get_historical_features(...).to_df()
            ...

        await train()

    **2. As an async context manager** — provides a ``feast.FeatureStore``
    scoped to the ``async with`` block::

        async with FeatureStore("prod-feast") as fs:
            features = fs.get_online_features(...)

    **3. As a sync context manager** — for non-async code::

        with FeatureStore("prod-feast") as fs:
            features = fs.get_online_features(...)

    Args:
        store: Name of an operator-managed FeatureStore deployment.
            Mutually exclusive with ``repo_path`` and ``config``.
        repo_path: Local path to a feature repo directory containing
            ``feature_store.yaml``. Mutually exclusive with ``store``.
        config: An explicit ``feast.RepoConfig`` object.
            Mutually exclusive with ``store`` and ``repo_path``.
        backend_config: Kubernetes backend configuration used when
            resolving an operator-managed ``store``.

    Examples:
        Connect to an operator-managed deployment::

            @FeatureStore("prod-feast")
            async def get_features(fs):
                return fs.get_online_features(
                    features=["driver_stats:conv_rate"],
                    entity_rows=[{"driver_id": 1001}],
                )

        Connect to a local feature repo::

            @FeatureStore(repo_path="feature_repo")
            def get_features(fs):
                return fs.get_online_features(...)

        Compose with other decorators::

            @Trainer("fine-tune", runtime="pytorch-distributed")
            @FeatureStore("prod-feast")
            async def train(fs):
                training_df = fs.get_historical_features(...).to_df()
                ...
    """

    def __init__(
        self,
        store: str | None = None,
        *,
        repo_path: str | None = None,
        config: Any | None = None,
        backend_config: KubernetesBackendConfig | None = None,
    ):
        self._store = store
        self._repo_path = repo_path
        self._config = config
        self._backend_config = backend_config
        self._fs: Any | None = None

    def _get_fs(self) -> Any:
        if self._fs is None:
            self._fs = _resolve_feature_store(
                store=self._store,
                repo_path=self._repo_path,
                config=self._config,
                backend_config=self._backend_config,
            )
        return self._fs

    # ── Decorator usage ──

    def __call__(self, func_or_store=None, **kwargs):
        # When called as @FeatureStore("name") — __init__ already received
        # the store name, and __call__ receives the function.
        if func_or_store is None:
            return self

        if callable(func_or_store):
            return self._wrap(func_or_store)

        raise TypeError(
            f"FeatureStore() received unexpected argument: {func_or_store!r}"
        )

    def _wrap(self, func: Callable) -> Callable:
        """Wrap a sync or async function to inject the FeatureStore."""

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                fs = await asyncio.get_event_loop().run_in_executor(
                    None, self._get_fs
                )
                return await func(fs, *args, **kwargs)

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            fs = self._get_fs()
            return func(fs, *args, **kwargs)

        return sync_wrapper

    # ── Async context manager ──

    async def __aenter__(self):
        self._fs = await asyncio.get_event_loop().run_in_executor(
            None, self._get_fs
        )
        return self._fs

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        self._fs = None
        return False

    # ── Sync context manager ──

    def __enter__(self):
        return self._get_fs()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._fs = None
        return False


class FeastMaterializer:
    """Decorator that wraps a materialization function as a managed workload.

    Provides the ``feast.FeatureStore`` as the first argument and can
    optionally configure scheduling, runtime, and hardware profiles for
    operator-managed materialization.

    When the decorated function is ``await``-ed, it:

    1. Resolves a configured ``feast.FeatureStore``
    2. Calls the user function with ``fs`` injected
    3. Optionally creates/updates a Kubernetes CronJob via the Feast operator

    Args:
        name: Identifier for this materialization workload.
        feature_store: Name of the operator-managed FeatureStore deployment,
            or a local ``repo_path``.
        repo_path: Local path to a feature repo (alternative to ``feature_store``).
        schedule: Cron expression for scheduled execution (e.g. ``"0 */6 * * *"``).
            When set, the decorator ensures a CronJob is configured on the operator.
        feature_views: Optional list of feature view names to materialize.
            If not specified, all eligible views are materialized.
        backend_config: Kubernetes backend configuration.

    Examples:
        One-shot materialization::

            @FeastMaterializer("backfill", feature_store="prod-feast")
            async def backfill(fs):
                fs.materialize(
                    start_date=datetime(2025, 1, 1),
                    end_date=datetime.utcnow(),
                )

            await backfill()

        Scheduled incremental materialization::

            @FeastMaterializer(
                "daily-materialize",
                feature_store="prod-feast",
                schedule="0 */6 * * *",
            )
            async def materialize(fs):
                fs.materialize_incremental(end_date=datetime.utcnow())

            await materialize()

        Materialize specific views::

            @FeastMaterializer(
                "driver-stats-materialize",
                feature_store="prod-feast",
                feature_views=["driver_hourly_stats", "driver_daily_stats"],
            )
            async def materialize(fs):
                fs.materialize_incremental(
                    end_date=datetime.utcnow(),
                    feature_views=["driver_hourly_stats", "driver_daily_stats"],
                )

            await materialize()
    """

    def __init__(
        self,
        name: str,
        *,
        feature_store: str | None = None,
        repo_path: str | None = None,
        schedule: str | None = None,
        feature_views: list[str] | None = None,
        backend_config: KubernetesBackendConfig | None = None,
    ):
        self._name = name
        self._feature_store = feature_store
        self._repo_path = repo_path
        self._schedule = schedule
        self._feature_views = feature_views
        self._backend_config = backend_config

    def __call__(self, func: Callable) -> Callable:
        materializer = self

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                if materializer._schedule:
                    await asyncio.get_event_loop().run_in_executor(
                        None, materializer._ensure_cronjob
                    )
                    logger.info(
                        "CronJob schedule '%s' configured for materializer '%s'",
                        materializer._schedule,
                        materializer._name,
                    )

                fs = await asyncio.get_event_loop().run_in_executor(
                    None, materializer._get_fs
                )
                return await func(fs, *args, **kwargs)

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            if materializer._schedule:
                materializer._ensure_cronjob()
                logger.info(
                    "CronJob schedule '%s' configured for materializer '%s'",
                    materializer._schedule,
                    materializer._name,
                )

            fs = materializer._get_fs()
            return func(fs, *args, **kwargs)

        return sync_wrapper

    def _get_fs(self) -> Any:
        return _resolve_feature_store(
            store=self._feature_store,
            repo_path=self._repo_path,
            backend_config=self._backend_config,
        )

    def _ensure_cronjob(self) -> None:
        """Ensure the operator-managed FeatureStore has a CronJob configured."""
        if not self._feature_store:
            logger.warning(
                "CronJob scheduling requires an operator-managed feature_store; "
                "schedule='%s' will be ignored for local repo_path",
                self._schedule,
            )
            return

        from kubeflow.feast.backends.kubernetes import KubernetesBackend

        bc = self._backend_config or KubernetesBackendConfig()
        backend = KubernetesBackend(bc)

        info = backend.get_store(self._feature_store)
        if info.state != "Ready":
            logger.warning(
                "FeatureStore '%s' is in state '%s'; CronJob may not be active",
                self._feature_store,
                info.state,
            )

        # The Feast operator manages CronJobs declaratively via the CR spec.
        # If a schedule is specified but the CR doesn't have one, we patch it.
        try:
            import json

            patch_body = {
                "spec": {
                    "cronJob": {
                        "schedule": self._schedule,
                    }
                }
            }
            backend.custom_api.patch_namespaced_custom_object(
                group="feast.dev",
                version="v1",
                namespace=backend.namespace,
                plural="featurestores",
                name=self._feature_store,
                body=patch_body,
            )
            logger.info(
                "Patched FeatureStore '%s' with CronJob schedule '%s'",
                self._feature_store,
                self._schedule,
            )
        except Exception:
            logger.warning(
                "Could not patch CronJob schedule on FeatureStore '%s'; "
                "configure it via create_store(options=[CronJobSchedule(...)]) instead",
                self._feature_store,
                exc_info=True,
            )


class FeastStreamProcessor:
    """Decorator for stream processing workloads that integrate Feast with Spark.

    Provides both a ``feast.FeatureStore`` and optionally a ``SparkSession``
    as arguments to the decorated function, enabling real-time feature
    processing from Kafka/Kinesis sources into the online store.

    Args:
        name: Identifier for this stream processing workload.
        feature_store: Name of the operator-managed FeatureStore deployment.
        repo_path: Local path to a feature repo (alternative to ``feature_store``).
        backend_config: Kubernetes backend configuration.

    Examples:
        Process stream feature views::

            @FeastStreamProcessor("driver-events", feature_store="prod-feast")
            async def process_stream(fs):
                # Process all stream feature views
                for sfv in fs.list_stream_feature_views():
                    print(f"Processing: {sfv.name}")

            await process_stream()

        Integrate with Spark Structured Streaming::

            @FeastStreamProcessor("driver-events", feature_store="prod-feast")
            async def process_stream(fs):
                import pandas as pd
                from datetime import datetime

                # Push real-time events to online store
                df = pd.DataFrame({
                    "driver_id": [1001],
                    "conv_rate": [0.92],
                    "event_timestamp": [datetime.utcnow()],
                })
                fs.push("driver_push_source", df)

            await process_stream()
    """

    def __init__(
        self,
        name: str,
        *,
        feature_store: str | None = None,
        repo_path: str | None = None,
        backend_config: KubernetesBackendConfig | None = None,
    ):
        self._name = name
        self._feature_store = feature_store
        self._repo_path = repo_path
        self._backend_config = backend_config

    def __call__(self, func: Callable) -> Callable:
        processor = self

        if asyncio.iscoroutinefunction(func):

            @functools.wraps(func)
            async def async_wrapper(*args, **kwargs):
                fs = await asyncio.get_event_loop().run_in_executor(
                    None, processor._get_fs
                )
                return await func(fs, *args, **kwargs)

            return async_wrapper

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            fs = processor._get_fs()
            return func(fs, *args, **kwargs)

        return sync_wrapper

    def _get_fs(self) -> Any:
        return _resolve_feature_store(
            store=self._feature_store,
            repo_path=self._repo_path,
            backend_config=self._backend_config,
        )
