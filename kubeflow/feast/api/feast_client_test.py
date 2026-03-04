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

"""Unit tests for FeastClient."""

from unittest.mock import Mock, patch

import pytest

from kubeflow.common.types import KubernetesBackendConfig
from kubeflow.feast.api.feast_client import FeastClient
from kubeflow.feast.types.types import (
    FeastProjectSource,
    FeastStoreInfo,
    FeastStoreState,
    OnlineStoreConfig,
    ServiceHostnames,
)


@pytest.fixture
def mock_backend():
    """Create a mock Kubernetes backend."""
    return Mock()


@pytest.fixture
def feast_client(mock_backend):
    """Create FeastClient with a mocked backend."""
    with (
        patch("kubernetes.config.load_kube_config", return_value=None),
        patch("kubernetes.client.CustomObjectsApi", return_value=Mock()),
        patch("kubernetes.client.CoreV1Api", return_value=Mock()),
    ):
        client = FeastClient(KubernetesBackendConfig())
        client.backend = mock_backend
        return client


def _make_store_info(
    name: str = "test-store",
    state: FeastStoreState = FeastStoreState.READY,
) -> FeastStoreInfo:
    return FeastStoreInfo(
        name=name,
        namespace="default",
        state=state,
        feast_project="test_project",
        feast_version="0.41.0",
        client_config_map=f"{name}-client",
        service_hostnames=ServiceHostnames(
            online_store=f"{name}-online.default.svc.cluster.local:80",
            registry=f"{name}-registry.default.svc.cluster.local:80",
        ),
    )


class TestFeastClientInit:
    def test_default_backend(self):
        with (
            patch("kubernetes.config.load_kube_config", return_value=None),
            patch("kubernetes.client.CustomObjectsApi", return_value=Mock()),
            patch("kubernetes.client.CoreV1Api", return_value=Mock()),
        ):
            client = FeastClient()
            assert client.backend is not None

    def test_invalid_backend_config(self):
        with pytest.raises(ValueError, match="Invalid backend config"):
            FeastClient(backend_config="invalid")


class TestCreateStore:
    def test_create_with_defaults(self, feast_client, mock_backend):
        mock_backend.create_store.return_value = _make_store_info(state=FeastStoreState.PENDING)
        mock_backend.wait_for_store_ready.return_value = _make_store_info()

        result = feast_client.create_store(feast_project="my_project")

        mock_backend.create_store.assert_called_once()
        mock_backend.wait_for_store_ready.assert_called_once()
        assert result.state == FeastStoreState.READY

    def test_create_without_wait(self, feast_client, mock_backend):
        mock_backend.create_store.return_value = _make_store_info(state=FeastStoreState.PENDING)

        result = feast_client.create_store(feast_project="my_project", wait=False)

        mock_backend.create_store.assert_called_once()
        mock_backend.wait_for_store_ready.assert_not_called()
        assert result.state == FeastStoreState.PENDING

    def test_create_with_all_params(self, feast_client, mock_backend):
        mock_backend.create_store.return_value = _make_store_info(state=FeastStoreState.PENDING)
        mock_backend.wait_for_store_ready.return_value = _make_store_info()

        result = feast_client.create_store(
            feast_project="my_project",
            project_source=FeastProjectSource(git_url="https://github.com/org/repo.git"),
            online_store=OnlineStoreConfig(persistence_type="redis"),
            timeout=600,
        )

        call_kwargs = mock_backend.create_store.call_args[1]
        assert call_kwargs["feast_project"] == "my_project"
        assert call_kwargs["project_source"].git_url == "https://github.com/org/repo.git"
        assert call_kwargs["online_store"].persistence_type == "redis"
        assert result.state == FeastStoreState.READY


class TestGetStore:
    def test_get_store(self, feast_client, mock_backend):
        mock_backend.get_store.return_value = _make_store_info()

        result = feast_client.get_store("test-store")

        mock_backend.get_store.assert_called_once_with("test-store")
        assert result.name == "test-store"


class TestListStores:
    def test_list_stores(self, feast_client, mock_backend):
        mock_backend.list_stores.return_value = [
            _make_store_info("store-1"),
            _make_store_info("store-2"),
        ]

        result = feast_client.list_stores()

        assert len(result) == 2
        assert result[0].name == "store-1"


class TestDeleteStore:
    def test_delete_store(self, feast_client, mock_backend):
        feast_client.delete_store("test-store")
        mock_backend.delete_store.assert_called_once_with("test-store")


class TestWaitForStoreReady:
    def test_wait_success(self, feast_client, mock_backend):
        mock_backend.wait_for_store_ready.return_value = _make_store_info()

        result = feast_client.wait_for_store_ready("test-store", timeout=60)

        mock_backend.wait_for_store_ready.assert_called_once_with(
            name="test-store", timeout=60, polling_interval=5,
        )
        assert result.state == FeastStoreState.READY


class TestGetClientConfig:
    def test_get_config(self, feast_client, mock_backend):
        mock_backend.get_client_config.return_value = "project: test\n"

        result = feast_client.get_client_config("test-store")

        assert result == "project: test\n"


class TestGetFeatureStore:
    def test_feature_store_import_error(self, feast_client, mock_backend):
        mock_backend.get_client_config.return_value = "project: test\n"

        with patch.dict("sys.modules", {"feast": None}):
            with pytest.raises(ImportError, match="feast is not installed"):
                feast_client.get_feature_store("test-store")

    def test_feature_store_success(self, feast_client, mock_backend):
        mock_backend.get_client_config.return_value = (
            "project: test_project\n"
            "registry:\n"
            "  registry_type: remote\n"
            "  path: registry.default.svc.cluster.local:80\n"
            "online_store:\n"
            "  type: remote\n"
            "  path: http://online.default.svc.cluster.local:80\n"
        )

        mock_fs = Mock()
        mock_load_config = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs) as mock_fs_cls,
            patch("feast.repo_config.load_repo_config", return_value=mock_load_config),
        ):
            result = feast_client.get_feature_store("test-store")
            mock_fs_cls.assert_called_once()


class TestApply:
    def test_apply(self, feast_client, mock_backend):
        mock_backend.get_client_config.return_value = "project: test\n"
        mock_fs = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config", return_value=Mock()),
        ):
            objects = [Mock(), Mock()]
            feast_client.apply("test-store", objects)
            mock_fs.apply.assert_called_once_with(objects, objects_to_delete=None)


class TestMaterialize:
    def test_materialize(self, feast_client, mock_backend):
        from datetime import datetime

        mock_backend.get_client_config.return_value = "project: test\n"
        mock_fs = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config", return_value=Mock()),
        ):
            start = datetime(2025, 1, 1)
            end = datetime(2025, 6, 1)
            feast_client.materialize("test-store", start, end)
            mock_fs.materialize.assert_called_once_with(
                start_date=start, end_date=end, feature_views=None,
            )


class TestGetOnlineFeatures:
    def test_get_online_features(self, feast_client, mock_backend):
        mock_backend.get_client_config.return_value = "project: test\n"
        mock_fs = Mock()
        mock_fs.get_online_features.return_value = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config", return_value=Mock()),
        ):
            feast_client.get_online_features(
                "test-store",
                features=["driver:conv_rate"],
                entity_rows=[{"driver_id": 1001}],
            )
            mock_fs.get_online_features.assert_called_once_with(
                features=["driver:conv_rate"],
                entity_rows=[{"driver_id": 1001}],
                full_feature_names=False,
            )


class TestGetHistoricalFeatures:
    def test_get_historical_features(self, feast_client, mock_backend):
        mock_backend.get_client_config.return_value = "project: test\n"
        mock_fs = Mock()
        mock_fs.get_historical_features.return_value = Mock()

        mock_df = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config", return_value=Mock()),
        ):
            feast_client.get_historical_features(
                "test-store",
                entity_df=mock_df,
                features=["driver:conv_rate"],
            )
            mock_fs.get_historical_features.assert_called_once_with(
                entity_df=mock_df,
                features=["driver:conv_rate"],
                full_feature_names=False,
            )


class TestPush:
    def test_push(self, feast_client, mock_backend):
        mock_backend.get_client_config.return_value = "project: test\n"
        mock_fs = Mock()
        mock_df = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config", return_value=Mock()),
            patch("feast.data_source.PushMode") as mock_push_mode,
        ):
            mock_push_mode.ONLINE = "ONLINE"
            feast_client.push("test-store", "my_source", mock_df)
            mock_fs.push.assert_called_once()

    def test_push_invalid_mode(self, feast_client, mock_backend):
        mock_backend.get_client_config.return_value = "project: test\n"
        mock_fs = Mock()
        mock_df = Mock()

        with (
            patch("feast.FeatureStore", return_value=mock_fs),
            patch("feast.repo_config.load_repo_config", return_value=Mock()),
            patch("feast.data_source.PushMode"),
        ):
            with pytest.raises(ValueError, match="Invalid push mode"):
                feast_client.push("test-store", "my_source", mock_df, to="invalid")
