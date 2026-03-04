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

"""Kubernetes backend for Feast operations."""

import logging
import multiprocessing
import time
import uuid

from kubernetes import client, config

from kubeflow.common import constants as common_constants
from kubeflow.common.types import KubernetesBackendConfig
from kubeflow.feast.backends.base import RuntimeBackend
from kubeflow.feast.constants import constants
from kubeflow.feast.types.types import (
    FeastProjectSource,
    FeastStoreInfo,
    FeastStoreState,
    OfflineStoreConfig,
    OnlineStoreConfig,
    RegistryConfig,
    ServiceHostnames,
)

logger = logging.getLogger(__name__)


def _generate_store_name() -> str:
    return f"feast-{uuid.uuid4().hex[:8]}"


def _get_store_info_from_cr(cr: dict) -> FeastStoreInfo:
    """Extract FeastStoreInfo from a raw FeatureStore CR dict."""
    metadata = cr.get("metadata", {})
    spec = cr.get("spec", {})
    status = cr.get("status", {})

    phase = status.get("phase", constants.FEAST_PENDING)
    try:
        state = FeastStoreState(phase)
    except ValueError:
        state = FeastStoreState.PENDING

    svc_hosts = status.get("serviceHostnames", {})
    hostnames = ServiceHostnames(
        online_store=svc_hosts.get("onlineStore") or None,
        offline_store=svc_hosts.get("offlineStore") or None,
        registry=svc_hosts.get("registry") or None,
        registry_rest=svc_hosts.get("registryRest") or None,
        ui=svc_hosts.get("ui") or None,
    )

    creation_ts = metadata.get("creationTimestamp")
    creation_dt = None
    if creation_ts:
        try:
            from datetime import datetime, timezone

            creation_dt = datetime.fromisoformat(creation_ts.replace("Z", "+00:00")).replace(
                tzinfo=timezone.utc
            )
        except (ValueError, TypeError):
            pass

    return FeastStoreInfo(
        name=metadata.get("name", ""),
        namespace=metadata.get("namespace", ""),
        state=state,
        feast_project=spec.get("feastProject"),
        feast_version=status.get("feastVersion"),
        client_config_map=status.get("clientConfigMap"),
        service_hostnames=hostnames,
        creation_timestamp=creation_dt,
    )


def _build_feature_store_cr(
    name: str,
    namespace: str,
    feast_project: str,
    project_source: FeastProjectSource | None = None,
    online_store: OnlineStoreConfig | None = None,
    offline_store: OfflineStoreConfig | None = None,
    registry: RegistryConfig | None = None,
) -> dict:
    """Build a FeatureStore custom resource dict."""
    cr: dict = {
        "apiVersion": f"{constants.FEAST_GROUP}/{constants.FEAST_VERSION}",
        "kind": constants.FEAST_KIND,
        "metadata": {
            "name": name,
            "namespace": namespace,
        },
        "spec": {
            "feastProject": feast_project,
        },
    }

    if project_source:
        if project_source.git_url:
            git_config: dict = {"url": project_source.git_url}
            if project_source.git_ref:
                git_config["ref"] = project_source.git_ref
            if project_source.feature_repo_path:
                git_config["featureRepoPath"] = project_source.feature_repo_path
            cr["spec"]["feastProjectDir"] = {"git": git_config}
        elif project_source.minimal_init:
            cr["spec"]["feastProjectDir"] = {"init": {"minimal": True}}

    services: dict = {}

    if online_store:
        online_spec: dict = {}
        if online_store.persistence_type:
            online_spec["persistence"] = {"store": {"type": online_store.persistence_type}}
        if online_store.image or online_store.resources:
            server_conf: dict = {}
            if online_store.image:
                server_conf["containerConfigs"] = {"image": online_store.image}
            if online_store.resources:
                res = {}
                if "cpu" in online_store.resources or "memory" in online_store.resources:
                    res["requests"] = {}
                    if "cpu" in online_store.resources:
                        res["requests"]["cpu"] = online_store.resources["cpu"]
                    if "memory" in online_store.resources:
                        res["requests"]["memory"] = online_store.resources["memory"]
                if res:
                    server_conf.setdefault("containerConfigs", {})["resources"] = res
            online_spec["server"] = server_conf
        if online_spec:
            services["onlineStore"] = online_spec

    if offline_store:
        offline_spec: dict = {}
        if offline_store.persistence_type:
            offline_spec["persistence"] = {"store": {"type": offline_store.persistence_type}}
        if offline_store.image or offline_store.resources:
            server_conf_off: dict = {}
            if offline_store.image:
                server_conf_off["containerConfigs"] = {"image": offline_store.image}
            if offline_store.resources:
                res_off = {}
                if "cpu" in offline_store.resources or "memory" in offline_store.resources:
                    res_off["requests"] = {}
                    if "cpu" in offline_store.resources:
                        res_off["requests"]["cpu"] = offline_store.resources["cpu"]
                    if "memory" in offline_store.resources:
                        res_off["requests"]["memory"] = offline_store.resources["memory"]
                if res_off:
                    server_conf_off.setdefault("containerConfigs", {})["resources"] = res_off
            offline_spec["server"] = server_conf_off
        if offline_spec:
            services["offlineStore"] = offline_spec

    if registry:
        registry_spec: dict = {}
        if registry.registry_type == "remote":
            registry_spec["remote"] = {}
        else:
            local_reg: dict = {}
            if registry.persistence_type:
                if registry.persistence_type == "sql":
                    local_reg["persistence"] = {"store": {"type": "sql"}}
                else:
                    local_reg["persistence"] = {"file": {"type": registry.persistence_type}}
            if registry.image or registry.resources:
                server_conf_reg: dict = {}
                if registry.image:
                    server_conf_reg["containerConfigs"] = {"image": registry.image}
                if registry.resources:
                    res_reg = {}
                    if "cpu" in registry.resources or "memory" in registry.resources:
                        res_reg["requests"] = {}
                        if "cpu" in registry.resources:
                            res_reg["requests"]["cpu"] = registry.resources["cpu"]
                        if "memory" in registry.resources:
                            res_reg["requests"]["memory"] = registry.resources["memory"]
                    if res_reg:
                        server_conf_reg.setdefault("containerConfigs", {})["resources"] = res_reg
                local_reg["server"] = server_conf_reg
            registry_spec["local"] = local_reg
        if registry_spec:
            services["registry"] = registry_spec

    if services:
        cr["spec"]["services"] = services

    return cr


class KubernetesBackend(RuntimeBackend):
    """Kubernetes backend for managing FeatureStore deployments via the Feast operator."""

    def __init__(self, backend_config: KubernetesBackendConfig):
        self.namespace = backend_config.namespace or "default"

        if backend_config.config_file:
            config.load_kube_config(config_file=backend_config.config_file)
        elif backend_config.context:
            config.load_kube_config(context=backend_config.context)
        else:
            try:
                config.load_incluster_config()
            except config.ConfigException:
                config.load_kube_config()

        self.custom_api = client.CustomObjectsApi()
        self.core_api = client.CoreV1Api()

    def _extract_name_option(self, options: list | None) -> tuple[str, list]:
        """Extract Name option from options list, or generate name if absent."""
        from kubeflow.feast.types.options import Name

        if not options:
            return _generate_store_name(), []

        name_from_option = None
        filtered_options = []

        for option in options:
            if isinstance(option, Name):
                name_from_option = option.name
            else:
                filtered_options.append(option)

        session_name = name_from_option if name_from_option else _generate_store_name()
        return session_name, filtered_options

    def create_store(
        self,
        feast_project: str,
        project_source: FeastProjectSource | None = None,
        online_store: OnlineStoreConfig | None = None,
        offline_store: OfflineStoreConfig | None = None,
        registry: RegistryConfig | None = None,
        options: list | None = None,
    ) -> FeastStoreInfo:
        name, filtered_options = self._extract_name_option(options)

        cr = _build_feature_store_cr(
            name=name,
            namespace=self.namespace,
            feast_project=feast_project,
            project_source=project_source,
            online_store=online_store,
            offline_store=offline_store,
            registry=registry,
        )

        for option in filtered_options:
            option(cr, self)

        logger.info("Creating FeatureStore '%s' in namespace '%s'", name, self.namespace)

        try:
            thread = self.custom_api.create_namespaced_custom_object(
                group=constants.FEAST_GROUP,
                version=constants.FEAST_VERSION,
                namespace=self.namespace,
                plural=constants.FEAST_PLURAL,
                body=cr,
                async_req=True,
            )
            response = thread.get(common_constants.DEFAULT_TIMEOUT)
        except multiprocessing.TimeoutError as e:
            raise TimeoutError(
                f"Timeout creating {constants.FEAST_KIND}: {self.namespace}/{name}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to create {constants.FEAST_KIND}: {self.namespace}/{name}"
            ) from e

        return _get_store_info_from_cr(response)

    def get_store(self, name: str) -> FeastStoreInfo:
        try:
            thread = self.custom_api.get_namespaced_custom_object(
                group=constants.FEAST_GROUP,
                version=constants.FEAST_VERSION,
                namespace=self.namespace,
                plural=constants.FEAST_PLURAL,
                name=name,
                async_req=True,
            )
            response = thread.get(common_constants.DEFAULT_TIMEOUT)
            return _get_store_info_from_cr(response)
        except multiprocessing.TimeoutError as e:
            raise TimeoutError(
                f"Timeout getting {constants.FEAST_KIND}: {self.namespace}/{name}"
            ) from e
        except client.ApiException as e:
            if e.status == 404:
                raise RuntimeError(
                    f"{constants.FEAST_KIND} not found: {self.namespace}/{name}"
                ) from e
            raise RuntimeError(
                f"Failed to get {constants.FEAST_KIND}: {self.namespace}/{name}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to get {constants.FEAST_KIND}: {self.namespace}/{name}"
            ) from e

    def list_stores(self) -> list[FeastStoreInfo]:
        try:
            thread = self.custom_api.list_namespaced_custom_object(
                group=constants.FEAST_GROUP,
                version=constants.FEAST_VERSION,
                namespace=self.namespace,
                plural=constants.FEAST_PLURAL,
                async_req=True,
            )
            response = thread.get(common_constants.DEFAULT_TIMEOUT)
        except multiprocessing.TimeoutError as e:
            raise TimeoutError(
                f"Timeout listing {constants.FEAST_KIND}s in namespace: {self.namespace}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to list {constants.FEAST_KIND}s in namespace: {self.namespace}"
            ) from e

        items = response.get("items", [])
        return [_get_store_info_from_cr(item) for item in items]

    def delete_store(self, name: str) -> None:
        try:
            thread = self.custom_api.delete_namespaced_custom_object(
                group=constants.FEAST_GROUP,
                version=constants.FEAST_VERSION,
                namespace=self.namespace,
                plural=constants.FEAST_PLURAL,
                name=name,
                async_req=True,
            )
            thread.get(common_constants.DEFAULT_TIMEOUT)
            logger.info("Deleted FeatureStore '%s'", name)
        except multiprocessing.TimeoutError as e:
            raise TimeoutError(
                f"Timeout deleting {constants.FEAST_KIND}: {self.namespace}/{name}"
            ) from e
        except client.ApiException as e:
            if e.status == 404:
                raise RuntimeError(
                    f"{constants.FEAST_KIND} not found: {self.namespace}/{name}"
                ) from e
            raise RuntimeError(
                f"Failed to delete {constants.FEAST_KIND}: {self.namespace}/{name}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to delete {constants.FEAST_KIND}: {self.namespace}/{name}"
            ) from e

    def wait_for_store_ready(
        self,
        name: str,
        timeout: int = 300,
        polling_interval: int = 5,
    ) -> FeastStoreInfo:
        start_time = time.time()
        last_log_time = 0.0

        while True:
            info = self.get_store(name)

            if info.state == FeastStoreState.READY:
                logger.info(
                    "FeatureStore ready: %s/%s (%.0fs)",
                    self.namespace,
                    name,
                    time.time() - start_time,
                )
                return info

            if info.state == FeastStoreState.FAILED:
                raise RuntimeError(
                    f"{constants.FEAST_KIND} failed: {self.namespace}/{name}"
                )

            now = time.time()
            if now - last_log_time >= 15.0:
                logger.info(
                    "Waiting for FeatureStore: %s/%s state=%s elapsed=%.0fs",
                    self.namespace,
                    name,
                    info.state,
                    now - start_time,
                )
                last_log_time = now

            if now - start_time >= timeout:
                raise TimeoutError(
                    f"Timeout waiting for {constants.FEAST_KIND} to be ready: "
                    f"{self.namespace}/{name} (timeout: {timeout}s)"
                )

            time.sleep(polling_interval)

    def get_client_config(self, name: str) -> str:
        info = self.get_store(name)
        config_map_name = info.client_config_map

        if not config_map_name:
            raise RuntimeError(
                f"No client config map found for {constants.FEAST_KIND}: "
                f"{self.namespace}/{name}. Ensure the store is in Ready state."
            )

        try:
            thread = self.core_api.read_namespaced_config_map(
                name=config_map_name,
                namespace=self.namespace,
                async_req=True,
            )
            cm = thread.get(common_constants.DEFAULT_TIMEOUT)
        except multiprocessing.TimeoutError as e:
            raise TimeoutError(
                f"Timeout reading ConfigMap: {self.namespace}/{config_map_name}"
            ) from e
        except client.ApiException as e:
            if e.status == 404:
                raise RuntimeError(
                    f"ConfigMap not found: {self.namespace}/{config_map_name}"
                ) from e
            raise RuntimeError(
                f"Failed to read ConfigMap: {self.namespace}/{config_map_name}"
            ) from e
        except Exception as e:
            raise RuntimeError(
                f"Failed to read ConfigMap: {self.namespace}/{config_map_name}"
            ) from e

        data = cm.data or {}
        yaml_content = data.get(constants.FEAST_CLIENT_CONFIG_KEY)
        if not yaml_content:
            raise RuntimeError(
                f"Key '{constants.FEAST_CLIENT_CONFIG_KEY}' not found in ConfigMap: "
                f"{self.namespace}/{config_map_name}"
            )

        return yaml_content
