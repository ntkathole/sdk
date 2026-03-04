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

"""Unit tests for Feast decorators and context managers."""

import asyncio
from unittest.mock import Mock, patch

import pytest

from kubeflow.feast.decorators import (
    FeastMaterializer,
    FeastStreamProcessor,
    FeatureStore,
    _resolve_feature_store,
)


# ── Helpers ──

def _mock_resolve(store=None, repo_path=None, config=None, backend_config=None):
    """Return a mock feast.FeatureStore."""
    mock_fs = Mock()
    mock_fs.project = "test_project"
    mock_fs.get_online_features.return_value = Mock(to_dict=Mock(return_value={"f": [1]}))
    mock_fs.get_historical_features.return_value = Mock(to_df=Mock(return_value="df"))
    return mock_fs


# ── FeatureStore decorator: sync ──

class TestFeatureStoreDecoratorSync:
    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_sync_decorator(self, mock_resolve):
        @FeatureStore("prod-feast")
        def my_func(fs):
            return fs.project

        result = my_func()
        assert result == "test_project"
        mock_resolve.assert_called_once()

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_sync_decorator_with_extra_args(self, mock_resolve):
        @FeatureStore("prod-feast")
        def my_func(fs, x, y=10):
            return (fs.project, x, y)

        result = my_func(42, y=99)
        assert result == ("test_project", 42, 99)

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_sync_decorator_with_repo_path(self, mock_resolve):
        @FeatureStore(repo_path="feature_repo")
        def my_func(fs):
            return fs.project

        result = my_func()
        assert result == "test_project"

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_sync_get_online_features(self, mock_resolve):
        @FeatureStore("prod-feast")
        def get_features(fs):
            return fs.get_online_features(
                features=["driver_stats:conv_rate"],
                entity_rows=[{"driver_id": 1001}],
            ).to_dict()

        result = get_features()
        assert result == {"f": [1]}


# ── FeatureStore decorator: async ──

class TestFeatureStoreDecoratorAsync:
    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_async_decorator(self, mock_resolve):
        @FeatureStore("prod-feast")
        async def my_func(fs):
            return fs.project

        result = asyncio.get_event_loop().run_until_complete(my_func())
        assert result == "test_project"

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_async_decorator_with_extra_args(self, mock_resolve):
        @FeatureStore("prod-feast")
        async def my_func(fs, x, y=10):
            return (fs.project, x, y)

        result = asyncio.get_event_loop().run_until_complete(my_func(42, y=99))
        assert result == ("test_project", 42, 99)


# ── FeatureStore context manager: sync ──

class TestFeatureStoreContextManagerSync:
    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_sync_context_manager(self, mock_resolve):
        with FeatureStore("prod-feast") as fs:
            assert fs.project == "test_project"

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_sync_context_manager_cleanup(self, mock_resolve):
        ctx = FeatureStore("prod-feast")
        with ctx as fs:
            assert fs is not None
        assert ctx._fs is None


# ── FeatureStore context manager: async ──

class TestFeatureStoreContextManagerAsync:
    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_async_context_manager(self, mock_resolve):
        async def run():
            async with FeatureStore("prod-feast") as fs:
                return fs.project

        result = asyncio.get_event_loop().run_until_complete(run())
        assert result == "test_project"

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_async_context_manager_cleanup(self, mock_resolve):
        ctx = FeatureStore("prod-feast")

        async def run():
            async with ctx as fs:
                assert fs is not None
            assert ctx._fs is None

        asyncio.get_event_loop().run_until_complete(run())


# ── FeastMaterializer ──

class TestFeastMaterializer:
    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_sync_materializer(self, mock_resolve):
        @FeastMaterializer("backfill", feature_store="prod-feast")
        def materialize(fs):
            return f"materialized {fs.project}"

        result = materialize()
        assert result == "materialized test_project"

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_async_materializer(self, mock_resolve):
        @FeastMaterializer("backfill", feature_store="prod-feast")
        async def materialize(fs):
            return f"materialized {fs.project}"

        result = asyncio.get_event_loop().run_until_complete(materialize())
        assert result == "materialized test_project"

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_materializer_with_schedule(self, mock_resolve):
        """Schedule doesn't break execution even if patching fails gracefully."""

        @FeastMaterializer(
            "daily",
            feature_store="prod-feast",
            schedule="0 */6 * * *",
        )
        def materialize(fs):
            return fs.project

        with patch.object(
            FeastMaterializer, "_ensure_cronjob", return_value=None
        ) as mock_cron:
            result = materialize()
            assert result == "test_project"
            mock_cron.assert_called_once()

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_materializer_with_repo_path(self, mock_resolve):
        @FeastMaterializer("local-mat", repo_path="feature_repo")
        def materialize(fs):
            return fs.project

        result = materialize()
        assert result == "test_project"


# ── FeastStreamProcessor ──

class TestFeastStreamProcessor:
    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_sync_processor(self, mock_resolve):
        @FeastStreamProcessor("driver-events", feature_store="prod-feast")
        def process(fs):
            return f"processing {fs.project}"

        result = process()
        assert result == "processing test_project"

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_async_processor(self, mock_resolve):
        @FeastStreamProcessor("driver-events", feature_store="prod-feast")
        async def process(fs):
            return f"processing {fs.project}"

        result = asyncio.get_event_loop().run_until_complete(process())
        assert result == "processing test_project"

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_processor_with_extra_args(self, mock_resolve):
        @FeastStreamProcessor("driver-events", feature_store="prod-feast")
        def process(fs, batch_size):
            return (fs.project, batch_size)

        result = process(100)
        assert result == ("test_project", 100)


# ── _resolve_feature_store ──

class TestResolveFeatureStore:
    def test_no_args_raises(self):
        with pytest.raises(ValueError, match="requires at least one"):
            _resolve_feature_store()

    def test_with_config(self):
        mock_config = Mock()
        mock_fs = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config"),
        ):
            result = _resolve_feature_store(config=mock_config)
            assert result is mock_fs

    def test_with_repo_path(self):
        mock_fs = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config"),
        ):
            result = _resolve_feature_store(repo_path="/path/to/repo")
            assert result is mock_fs

    def test_with_store_name(self):
        mock_fs = Mock()
        mock_backend = Mock()
        mock_backend.get_client_config.return_value = "project: test\n"

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config", return_value=Mock()),
            patch(
                "kubeflow.feast.backends.kubernetes.KubernetesBackend",
                return_value=mock_backend,
            ),
        ):
            result = _resolve_feature_store(store="prod-feast")
            assert result is mock_fs

    def test_feast_not_installed(self):
        with patch.dict("sys.modules", {"feast": None}):
            with pytest.raises(ImportError, match="feast is not installed"):
                _resolve_feature_store(repo_path="/path")


# ── Composability (decorator stacking) ──

class TestComposability:
    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_two_decorators_stacked(self, mock_resolve):
        """Simulate stacking an outer decorator with @FeatureStore."""

        def mock_trainer_decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        import functools

        @mock_trainer_decorator
        @FeatureStore("prod-feast")
        def train(fs, learning_rate=0.01):
            return (fs.project, learning_rate)

        result = train(learning_rate=0.1)
        assert result == ("test_project", 0.1)

    @patch("kubeflow.feast.decorators._resolve_feature_store", side_effect=_mock_resolve)
    def test_async_decorator_stacking(self, mock_resolve):
        """Simulate stacked async decorators."""

        def mock_trainer_decorator(func):
            @functools.wraps(func)
            async def wrapper(*args, **kwargs):
                return await func(*args, **kwargs)

            return wrapper

        import functools

        @mock_trainer_decorator
        @FeatureStore("prod-feast")
        async def train(fs, lr=0.01):
            return (fs.project, lr)

        result = asyncio.get_event_loop().run_until_complete(train(lr=0.1))
        assert result == ("test_project", 0.1)
