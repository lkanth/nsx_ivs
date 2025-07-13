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


def get_vlans(suite_api_client: SuiteApiClient) -> Any:
    switch_by_name = {}  
    try:
        logger.info(f'Making VCF Operations REST API call to retrieve a list of NSX Logical Switches')
        switches: List[Object] = suite_api_client.query_for_resources(
            {
                "adapterKind": ["NSXTAdapter"],
                "resourceKind": ["LogicalSwitch"]
            }
        )
        logger.info(f'VCF Operations REST API call returned response with {len(switches)} Logical Switch objects')      
        for switch in switches:
            if (re.findall("vlan-", switch.get_key().name)):
                switch_by_name.update({re.split("vlan-", switch.get_key().name,1)[1]: switch})
    except Exception as e:
        logger.error(f'Exception occured while getting a list of Logical Switches from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}') 
    
    return switch_by_name
    
def get_switch_property(suite_api_client: SuiteApiClient, switch: Object, property: str) -> str:
    try:
        logger.info(f'Making VCF Operations REST API call to retrieve Logical Switch resource identifier')
        response = suite_api_client.get(f'/api/resources?name={switch.get_key().name}&resourceKind=LogicalSwitch&_no_links=true')
        resource_id = json.loads(response.content)["resourceList"][0]["identifier"]
        logger.info(f'Response from VCF Operations REST API call - Logical Switch resource identifier is: {resource_id}')
        
        logger.info(f'Making VCF Operations REST API call to retrieve Logical Switch properties')
        propResponse = suite_api_client.get(f'/api/resources/{resource_id}/properties?_no_links=true')
        logger.info(f'Retrieved Logical Switch properties from VCF Operations')
        properties_list = json.loads(propResponse.content)["property"]
        for prop in properties_list:
            if prop["name"] == property:
                return prop["value"]
    except Exception as e:
        logger.error(f'Exception occured while getting Logical Switch property values from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}')
    return None
