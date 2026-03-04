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

"""Unit tests for Feast KubernetesBackend."""

import multiprocessing
from unittest.mock import Mock, patch

from kubernetes.client import ApiException
import pytest

from kubeflow.common.types import KubernetesBackendConfig
from kubeflow.feast.backends.kubernetes.backend import KubernetesBackend, _build_feature_store_cr
from kubeflow.feast.constants import constants
from kubeflow.feast.test.common import (
    DEFAULT_NAMESPACE,
    FAILED,
    FEAST_STORE_FAILED,
    FEAST_STORE_PENDING,
    FEAST_STORE_READY,
    SUCCESS,
    TIMEOUT,
    TestCase,
)
from kubeflow.feast.types.options import Annotations, CronJobSchedule, Labels, Name, Replicas
from kubeflow.feast.types.types import (
    FeastProjectSource,
    FeastStoreInfo,
    FeastStoreState,
    OfflineStoreConfig,
    OnlineStoreConfig,
    RegistryConfig,
)

# --------------------------
# Fixtures
# --------------------------


@pytest.fixture
def kubernetes_backend():
    """Provide KubernetesBackend with mocked K8s APIs."""
    with (
        patch("kubernetes.config.load_kube_config", return_value=None),
        patch(
            "kubernetes.client.CustomObjectsApi",
            return_value=Mock(
                create_namespaced_custom_object=Mock(side_effect=_mock_create),
                get_namespaced_custom_object=Mock(side_effect=_mock_get),
                list_namespaced_custom_object=Mock(side_effect=_mock_list),
                delete_namespaced_custom_object=Mock(side_effect=_mock_delete),
            ),
        ),
        patch(
            "kubernetes.client.CoreV1Api",
            return_value=Mock(
                read_namespaced_config_map=Mock(side_effect=_mock_read_config_map),
            ),
        ),
    ):
        yield KubernetesBackend(KubernetesBackendConfig())


# --------------------------
# Mock Handlers
# --------------------------


def create_mock_thread(response=None):
    """Create mock thread that returns response on .get()."""
    mock_thread = Mock()
    mock_thread.get.return_value = response
    return mock_thread


def create_error_thread(exc: Exception):
    """Create mock thread whose .get() raises the given exception."""
    mock_thread = Mock()
    mock_thread.get.side_effect = exc
    return mock_thread


def get_feast_store_cr(
    name: str,
    namespace: str = DEFAULT_NAMESPACE,
    phase: str = "Pending",
    feast_project: str = "test_project",
    client_config_map: str | None = None,
    service_hostnames: dict | None = None,
) -> dict:
    """Create a mock FeatureStore CR dict for testing."""
    cr = {
        "apiVersion": f"{constants.FEAST_GROUP}/{constants.FEAST_VERSION}",
        "kind": constants.FEAST_KIND,
        "metadata": {
            "name": name,
            "namespace": namespace,
            "creationTimestamp": "2025-06-15T10:30:00Z",
        },
        "spec": {
            "feastProject": feast_project,
        },
        "status": {
            "phase": phase,
            "feastVersion": "0.41.0",
        },
    }
    if client_config_map:
        cr["status"]["clientConfigMap"] = client_config_map
    if service_hostnames:
        cr["status"]["serviceHostnames"] = service_hostnames
    return cr


def _mock_create(**kwargs):
    body = kwargs.get("body", {})
    name = body.get("metadata", {}).get("name", "unknown")
    namespace = body.get("metadata", {}).get("namespace", DEFAULT_NAMESPACE)
    feast_project = body.get("spec", {}).get("feastProject", "test_project")

    if name == FEAST_STORE_FAILED:
        return create_error_thread(RuntimeError("Creation failed"))
    if name == "timeout-store":
        return create_error_thread(multiprocessing.TimeoutError("Timeout"))

    cr = get_feast_store_cr(
        name=name,
        namespace=namespace,
        feast_project=feast_project,
        phase="Pending",
    )
    return create_mock_thread(cr)


def _mock_get(**kwargs):
    name = kwargs.get("name", "")
    namespace = kwargs.get("namespace", DEFAULT_NAMESPACE)

    if name == FEAST_STORE_READY:
        cr = get_feast_store_cr(
            name=name,
            namespace=namespace,
            phase="Ready",
            client_config_map=f"{name}-client",
            service_hostnames={
                "onlineStore": f"{name}-online.{namespace}.svc.cluster.local:80",
                "registry": f"{name}-registry.{namespace}.svc.cluster.local:80",
            },
        )
        return create_mock_thread(cr)

    if name == FEAST_STORE_PENDING:
        cr = get_feast_store_cr(name=name, namespace=namespace, phase="Pending")
        return create_mock_thread(cr)

    if name == FEAST_STORE_FAILED:
        cr = get_feast_store_cr(name=name, namespace=namespace, phase="Failed")
        return create_mock_thread(cr)

    if name == "not-found":
        return create_error_thread(ApiException(status=404, reason="Not Found"))

    if name == "timeout-store":
        return create_error_thread(multiprocessing.TimeoutError("Timeout"))

    cr = get_feast_store_cr(name=name, namespace=namespace, phase="Pending")
    return create_mock_thread(cr)


def _mock_list(**kwargs):
    items = [
        get_feast_store_cr("store-1", phase="Ready"),
        get_feast_store_cr("store-2", phase="Pending"),
    ]
    return create_mock_thread({"items": items})


def _mock_delete(**kwargs):
    name = kwargs.get("name", "")
    if name == "not-found":
        return create_error_thread(ApiException(status=404, reason="Not Found"))
    if name == "timeout-store":
        return create_error_thread(multiprocessing.TimeoutError("Timeout"))
    return create_mock_thread()


def _mock_read_config_map(**kwargs):
    name = kwargs.get("name", "")
    if name == f"{FEAST_STORE_READY}-client":
        cm = Mock()
        cm.data = {
            constants.FEAST_CLIENT_CONFIG_KEY: (
                "project: test_project\n"
                "registry:\n"
                "  registry_type: remote\n"
                "  path: feast-store-ready-registry.default.svc.cluster.local:80\n"
                "online_store:\n"
                "  type: remote\n"
                "  path: http://feast-store-ready-online.default.svc.cluster.local:80\n"
            ),
        }
        return create_mock_thread(cm)
    if name == "not-found-client":
        return create_error_thread(ApiException(status=404, reason="Not Found"))
    return create_error_thread(ApiException(status=404, reason="Not Found"))


# --------------------------
# Tests: create_store
# --------------------------


class TestCreateStore:
    test_cases = [
        TestCase(
            name="Create with defaults",
            expected_status=SUCCESS,
            config={"feast_project": "my_project"},
        ),
        TestCase(
            name="Create with git source",
            expected_status=SUCCESS,
            config={
                "feast_project": "my_project",
                "project_source": FeastProjectSource(
                    git_url="https://github.com/org/repo.git",
                    git_ref="main",
                ),
            },
        ),
        TestCase(
            name="Create with online store config",
            expected_status=SUCCESS,
            config={
                "feast_project": "my_project",
                "online_store": OnlineStoreConfig(persistence_type="redis"),
            },
        ),
        TestCase(
            name="Create with name option",
            expected_status=SUCCESS,
            config={
                "feast_project": "my_project",
                "options": [Name("custom-feast")],
            },
        ),
        TestCase(
            name="Create with labels and annotations",
            expected_status=SUCCESS,
            config={
                "feast_project": "my_project",
                "options": [
                    Labels({"team": "ml"}),
                    Annotations({"description": "test store"}),
                ],
            },
        ),
        TestCase(
            name="Create with cron job",
            expected_status=SUCCESS,
            config={
                "feast_project": "my_project",
                "options": [CronJobSchedule("0 * * * *")],
            },
        ),
    ]

    @pytest.mark.parametrize(
        "test_case", test_cases, ids=[tc.name for tc in test_cases]
    )
    def test_create_store(self, kubernetes_backend, test_case):
        result = kubernetes_backend.create_store(**test_case.config)
        assert isinstance(result, FeastStoreInfo)
        assert result.namespace == DEFAULT_NAMESPACE
        assert result.state == FeastStoreState.PENDING


# --------------------------
# Tests: get_store
# --------------------------


class TestGetStore:
    test_cases = [
        TestCase(
            name="Get ready store",
            expected_status=SUCCESS,
            config={"name": FEAST_STORE_READY},
        ),
        TestCase(
            name="Get pending store",
            expected_status=SUCCESS,
            config={"name": FEAST_STORE_PENDING},
        ),
        TestCase(
            name="Get non-existent store",
            expected_status=FAILED,
            config={"name": "not-found"},
            expected_error=RuntimeError,
        ),
        TestCase(
            name="Get with timeout",
            expected_status=TIMEOUT,
            config={"name": "timeout-store"},
            expected_error=TimeoutError,
        ),
    ]

    @pytest.mark.parametrize(
        "test_case", test_cases, ids=[tc.name for tc in test_cases]
    )
    def test_get_store(self, kubernetes_backend, test_case):
        if test_case.expected_error:
            with pytest.raises(test_case.expected_error):
                kubernetes_backend.get_store(**test_case.config)
        else:
            result = kubernetes_backend.get_store(**test_case.config)
            assert isinstance(result, FeastStoreInfo)
            assert result.name == test_case.config["name"]


# --------------------------
# Tests: list_stores
# --------------------------


class TestListStores:
    def test_list_stores(self, kubernetes_backend):
        result = kubernetes_backend.list_stores()
        assert isinstance(result, list)
        assert len(result) == 2
        assert all(isinstance(s, FeastStoreInfo) for s in result)


# --------------------------
# Tests: delete_store
# --------------------------


class TestDeleteStore:
    test_cases = [
        TestCase(
            name="Delete existing store",
            expected_status=SUCCESS,
            config={"name": FEAST_STORE_READY},
        ),
        TestCase(
            name="Delete non-existent store",
            expected_status=FAILED,
            config={"name": "not-found"},
            expected_error=RuntimeError,
        ),
        TestCase(
            name="Delete with timeout",
            expected_status=TIMEOUT,
            config={"name": "timeout-store"},
            expected_error=TimeoutError,
        ),
    ]

    @pytest.mark.parametrize(
        "test_case", test_cases, ids=[tc.name for tc in test_cases]
    )
    def test_delete_store(self, kubernetes_backend, test_case):
        if test_case.expected_error:
            with pytest.raises(test_case.expected_error):
                kubernetes_backend.delete_store(**test_case.config)
        else:
            kubernetes_backend.delete_store(**test_case.config)


# --------------------------
# Tests: wait_for_store_ready
# --------------------------


class TestWaitForStoreReady:
    def test_wait_ready_store(self, kubernetes_backend):
        result = kubernetes_backend.wait_for_store_ready(FEAST_STORE_READY)
        assert result.state == FeastStoreState.READY

    def test_wait_failed_store(self, kubernetes_backend):
        with pytest.raises(RuntimeError, match="failed"):
            kubernetes_backend.wait_for_store_ready(FEAST_STORE_FAILED)

    def test_wait_timeout(self, kubernetes_backend):
        with pytest.raises(TimeoutError):
            kubernetes_backend.wait_for_store_ready(
                FEAST_STORE_PENDING, timeout=1, polling_interval=0.1
            )


# --------------------------
# Tests: get_client_config
# --------------------------


class TestGetClientConfig:
    def test_get_config_success(self, kubernetes_backend):
        config_yaml = kubernetes_backend.get_client_config(FEAST_STORE_READY)
        assert "project: test_project" in config_yaml
        assert "registry_type: remote" in config_yaml

    def test_get_config_no_config_map(self, kubernetes_backend):
        with pytest.raises(RuntimeError, match="No client config map"):
            kubernetes_backend.get_client_config(FEAST_STORE_PENDING)


# --------------------------
# Tests: _build_feature_store_cr
# --------------------------


class TestBuildFeatureStoreCR:
    def test_minimal_cr(self):
        cr = _build_feature_store_cr(
            name="test", namespace="default", feast_project="my_project"
        )
        assert cr["metadata"]["name"] == "test"
        assert cr["spec"]["feastProject"] == "my_project"
        assert "services" not in cr["spec"]

    def test_cr_with_git_source(self):
        source = FeastProjectSource(
            git_url="https://github.com/org/repo.git",
            git_ref="main",
            feature_repo_path="features",
        )
        cr = _build_feature_store_cr(
            name="test",
            namespace="default",
            feast_project="my_project",
            project_source=source,
        )
        git = cr["spec"]["feastProjectDir"]["git"]
        assert git["url"] == "https://github.com/org/repo.git"
        assert git["ref"] == "main"
        assert git["featureRepoPath"] == "features"

    def test_cr_with_minimal_init(self):
        source = FeastProjectSource(minimal_init=True)
        cr = _build_feature_store_cr(
            name="test",
            namespace="default",
            feast_project="my_project",
            project_source=source,
        )
        assert cr["spec"]["feastProjectDir"]["init"]["minimal"] is True

    def test_cr_with_online_store(self):
        online = OnlineStoreConfig(persistence_type="redis")
        cr = _build_feature_store_cr(
            name="test",
            namespace="default",
            feast_project="my_project",
            online_store=online,
        )
        assert cr["spec"]["services"]["onlineStore"]["persistence"]["store"]["type"] == "redis"

    def test_cr_with_offline_store(self):
        offline = OfflineStoreConfig(persistence_type="duckdb")
        cr = _build_feature_store_cr(
            name="test",
            namespace="default",
            feast_project="my_project",
            offline_store=offline,
        )
        assert cr["spec"]["services"]["offlineStore"]["persistence"]["store"]["type"] == "duckdb"

    def test_cr_with_registry(self):
        registry = RegistryConfig(registry_type="local", persistence_type="sql")
        cr = _build_feature_store_cr(
            name="test",
            namespace="default",
            feast_project="my_project",
            registry=registry,
        )
        assert cr["spec"]["services"]["registry"]["local"]["persistence"]["store"]["type"] == "sql"

    def test_cr_with_remote_registry(self):
        registry = RegistryConfig(registry_type="remote")
        cr = _build_feature_store_cr(
            name="test",
            namespace="default",
            feast_project="my_project",
            registry=registry,
        )
        assert "remote" in cr["spec"]["services"]["registry"]


# --------------------------
# Tests: Options
# --------------------------


class TestOptions:
    def test_labels_option(self, kubernetes_backend):
        cr = _build_feature_store_cr("t", "default", "p")
        Labels({"team": "ml"})( cr, kubernetes_backend)
        assert cr["metadata"]["labels"]["team"] == "ml"

    def test_annotations_option(self, kubernetes_backend):
        cr = _build_feature_store_cr("t", "default", "p")
        Annotations({"desc": "test"})( cr, kubernetes_backend)
        assert cr["metadata"]["annotations"]["desc"] == "test"

    def test_cron_job_option(self, kubernetes_backend):
        cr = _build_feature_store_cr("t", "default", "p")
        CronJobSchedule("0 * * * *")(cr, kubernetes_backend)
        assert cr["spec"]["cronJob"]["schedule"] == "0 * * * *"

    def test_replicas_option(self, kubernetes_backend):
        cr = _build_feature_store_cr("t", "default", "p")
        Replicas(3)(cr, kubernetes_backend)
        assert cr["spec"]["replicas"] == 3

    def test_name_option(self, kubernetes_backend):
        cr = _build_feature_store_cr("t", "default", "p")
        Name("custom-name")(cr, kubernetes_backend)
        assert cr["metadata"]["name"] == "custom-name"
