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
import re

logger = logging.getLogger(__name__)


def get_vlans(
    suite_api_client: SuiteApiClient
) -> Any:
    try:
        switches: List[Object] = suite_api_client.query_for_resources(
            {
                "adapterKind": ["NSXTAdapter"],
                "resourceKind": ["LogicalSwitch"]
            }
        )

        switch_by_name = {}

        for switch in switches:
            if (re.findall("vlan-", switch.get_key().name)):
                switch_by_name.update({re.split("vlan-", switch.get_key().name,1)[1]: switch})
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None
#    logger.info(switch_by_name)
    return switch_by_name
    
def get_switch_property(
    suite_api_client: SuiteApiClient,
    switch: Object,
    property: str
) -> str:
    try:
        response = suite_api_client.get(f'/api/resources?name={switch.get_key().name}&resourceKind=LogicalSwitch&_no_links=true')
        resource_id = json.loads(response.content)["resourceList"][0]["identifier"]
        
        response = suite_api_client.get(f'/api/resources/{resource_id}/properties?_no_links=true')
        properties_list = json.loads(response.content)["property"]
        for prop in properties_list:
            if prop["name"] == property:
                return prop["value"]
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return None
    
    logger.info(f'Response: {response.content}')
    return None
