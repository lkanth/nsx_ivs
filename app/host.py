#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import json
from typing import Any
from typing import List

from aria.ops.object import Object
from aria.ops.result import CollectResult
from aria.ops.suite_api_client import SuiteApiClient
from constants import VCENTER_ADAPTER_KIND
from pyVmomi import vim

logger = logging.getLogger(__name__)


def get_hosts(
    suite_api_client: SuiteApiClient,
    adapter_instance_id: str,
    content: Any,  # vim.ServiceInstanceContent
) -> Any:
    container = content.rootFolder  # starting point to look into
    view_type = [vim.HostSystem]  # object types to look for
    recursive = True  # whether we should look into it recursively
    container_view = content.viewManager.CreateContainerView(
        container, view_type, recursive
    )
    result = []
    logger.info(f'Adapter instance: {adapter_instance_id}')
    respone = suite_api_client.get(f'/api/resources/')
    hosts: List[Object] = suite_api_client.query_for_resources(
        {
            "adapterKind": [VCENTER_ADAPTER_KIND],
            "resourceKind": ["HostSystem"],
            "adapterInstanceId": [adapter_instance_id],
        }
    )

    hosts_by_name: dict[str, Object] = {
        f"vim.HostSystem:{host.get_identifier_value('VMEntityObjectID')}": host
        for host in hosts
    }

    children = container_view.view
    for host_system in children:
        h = repr(host_system.config.host).strip("'")  # Remove quotes
        host = hosts_by_name.get(h)
        if host:
            result.append(host)
        else:
            logger.warning(
                f"Could not find HostSystem {host_system.config.summary.name} with id '{h}'."
            )
    return result
    
def get_host_property(
    suite_api_client: SuiteApiClient,
    host: Object,
    property: str
) -> str:
    try:
        host_response = suite_api_client.get(f'/api/resources?name={host.get_key().name}&resourceKind=HostSystem&_no_links=true')
        host_id = json.loads(host_response.content)["resourceList"][0]["identifier"]
        
        response = suite_api_client.get(f'/api/resources/{host_id}/properties?_no_links=true')
        properties_list = json.loads(response.content)["property"]
        for prop in properties_list:
            if prop["name"] == property:
                return prop["value"]
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None
    
    logger.info(f'Response: {response.content}')
    return None


def add_host_metrics(
    suite_api_client: SuiteApiClient,
    adapter_instance_id: str,
    result: CollectResult,
    content: Any,  # vim.ServiceInstanceContent
) -> None:
    container = content.rootFolder  # starting point to look into
    view_type = [vim.HostSystem]  # object types to look for
    recursive = True  # whether we should look into it recursively
    container_view = content.viewManager.CreateContainerView(
        container, view_type, recursive
    )

    hosts: List[Object] = suite_api_client.query_for_resources(
        {
            "adapterKind": [VCENTER_ADAPTER_KIND],
            "resourceKind": ["HostSystem"],
            "adapterInstanceId": [adapter_instance_id],
        }
    )

    hosts_by_name: dict[str, Object] = {
        f"vim.HostSystem:{host.get_identifier_value('VMEntityObjectID')}": host
        for host in hosts
    }

    children = container_view.view
    for host_system in children:
        h = repr(host_system.config.host).strip("'")  # Remove quotes
        host = hosts_by_name.get(h)
        if host:
            for stack in host_system.config.network.netStackInstance:
                logger.info(f"net|Network Stack:{stack.key}|TCP/IP Stack Type")
                host.with_property(
                    f"net|Network Stack:{stack.key}|TCP/IP Stack Type", stack.key
                )
                host.with_property(
                    f"net|Network Stack:{stack.key}|VMkernel Gateway IP",
                    str(stack.ipRouteConfig.defaultGateway),
                )
                host.with_property(
                    f"net|Network Stack:{stack.key}|Gateway Is Configured",
                    bool(stack.ipRouteConfig.defaultGateway),
                )
            result.add_object(host)
        else:
            logger.warning(
                f"Could not find HostSystem {host_system.config.summary.name} with id '{h}'."
            )