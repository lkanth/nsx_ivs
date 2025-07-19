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
from constants import VCFOPS_VLANID_PROPERTY_KEY
import re

logger = logging.getLogger(__name__)


def get_vlans(suite_api_client: SuiteApiClient, adapter_instance_id: str) -> Any:
    vlansDict = {}
    try:
        logger.info(f'Making VCF Operations REST API call to retrieve a list of NSX Logical Switches')
        distPortGroups: List[Object] = suite_api_client.query_for_resources(
            {
                "adapterKind": [VCENTER_ADAPTER_KIND],
                "resourceKind": ["DistributedVirtualPortgroup"],
                "adapterInstanceId": [adapter_instance_id]
            }
        )
        logger.info(f'VCF Operations REST API call returned response with {len(distPortGroups)} distributed port group objects')
        for distPortGroup in distPortGroups:
            vlanID = getDistPortGroupProperty(suite_api_client, distPortGroup, VCFOPS_VLANID_PROPERTY_KEY)
            if vlanID is not None and vlanID != '' and vlanID.casefold() != 'none'.casefold():
                vlansDict[vlanID] = distPortGroup
            else:
                vlansDict[distPortGroup.get_key().name] = distPortGroup
        logger.info(f'VLAN IDs retrieved {vlansDict}')  

    except Exception as e:
        logger.error(f'Exception occured while getting a list of distributed port group objects from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}') 
    
    return vlansDict
    
def getDistPortGroupProperty(suite_api_client: SuiteApiClient, distPortGroup: Object, property: str) -> str:
    try:
        logger.info(f'Making VCF Operations REST API call to retrieve distributed port group resource identifier')
        response = suite_api_client.get(f'/api/resources?name={distPortGroup.get_key().name}&resourceKind=DistributedVirtualPortgroup&_no_links=true')
        resource_id = json.loads(response.content)["resourceList"][0]["identifier"]
        logger.info(f'Response from VCF Operations REST API call - Distributed Port group resource identifier is: {resource_id}')
        if not resource_id:
            logger.info(f'VLAN resource identifier cannot be empty or NULL')
            return None
        
        logger.info(f'Making VCF Operations REST API call to retrieve distributed port group properties')
        propResponse = suite_api_client.get(f'/api/resources/{resource_id}/properties?_no_links=true')
        logger.info(f'Retrieved distributed port group properties from VCF Operations')
        properties_list = json.loads(propResponse.content)["property"]
        for prop in properties_list:
            if prop["name"] == property:
                return prop["value"]
    except Exception as e:
        logger.error(f'Exception occured while getting Logical Switch property values from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}')
    return None
