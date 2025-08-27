#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import json
from typing import Any
from typing import List
from pyVmomi import vim

from aria.ops.object import Object
from aria.ops.suite_api_client import SuiteApiClient
from constants import VCENTER_ADAPTER_KIND
from constants import VCFOPS_DISTPORTGROUP_VLANID_PROPERTY_KEY
from constants import DISTPORTGROUP_NSXIVS_NUMNODES_RELATED
from constants import DISTPORTGROUP_NSXIVS_HAS_RELATED_DISCONNECTED
from constants import DISTPORTGROUP_NSXIVS_RELATED_NODES
from constants import DISTPORTGROUP_NSXIVS_RELATED_NODES_DISCONNECTED


logger = logging.getLogger(__name__)


def get_vlans(suite_api_client: SuiteApiClient, adapter_instance_id: str, content, switchesByUUID: dict) -> Any:
    vlansDict = {}
    portGroupSwitchDict = {}
    try:
        portGroupSwitchDict = get_vcenter_switch_portgroups(suite_api_client,adapter_instance_id, content)
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
            entryDict = {}
            
            if distPortGroup.get_key().name in portGroupSwitchDict and portGroupSwitchDict[distPortGroup.get_key().name]:
                entryDict['switchUUID'] = portGroupSwitchDict[distPortGroup.get_key().name]
                entryDict['DistPortGroupObject'] = distPortGroup
            
            if entryDict and len(entryDict) > 0:
                if entryDict['switchUUID'] in switchesByUUID and entryDict['switchUUID']:
                    distPortGroup.add_parent(switchesByUUID.get(entryDict['switchUUID']))
                vlanID = get_distportgroup_property(suite_api_client, distPortGroup, VCFOPS_DISTPORTGROUP_VLANID_PROPERTY_KEY)
                if vlanID is not None and vlanID != '' and vlanID.casefold() != 'none'.casefold():
                    
                    numRelatedNodesProp = get_distportgroup_property(suite_api_client, distPortGroup, DISTPORTGROUP_NSXIVS_NUMNODES_RELATED)
                    if numRelatedNodesProp is None or numRelatedNodesProp == '':
                        entryDict['numRelatedNodesProp'] = 0
                    else:
                        entryDict['numRelatedNodesProp'] = int(float(numRelatedNodesProp))
                    
                    entryDict['currentNumRelatedNodes'] = 0
                    entryDict['currentRelatedNodes'] = ""
                    
                    hasSomeRelatedNodesDisconnectedProp = get_distportgroup_property(suite_api_client, distPortGroup, DISTPORTGROUP_NSXIVS_HAS_RELATED_DISCONNECTED)
                    if hasSomeRelatedNodesDisconnectedProp:
                        entryDict['hasSomeRelatedNodesDisconnectedProp'] = hasSomeRelatedNodesDisconnectedProp
                    else:
                        entryDict['hasSomeRelatedNodesDisconnectedProp'] = "NO"
                    
                    relatedVLANNodesProp = get_distportgroup_property(suite_api_client, distPortGroup, DISTPORTGROUP_NSXIVS_RELATED_NODES)
                    if relatedVLANNodesProp:
                        entryDict['relatedVLANNodesProp'] = relatedVLANNodesProp
                    else:
                        entryDict['relatedVLANNodesProp'] = ''
                    
                    relatedVLANNodesDisconnectedProp = get_distportgroup_property(suite_api_client, distPortGroup, DISTPORTGROUP_NSXIVS_RELATED_NODES_DISCONNECTED)
                    if relatedVLANNodesDisconnectedProp:
                        entryDict['relatedVLANNodesDisconnectedProp'] = relatedVLANNodesDisconnectedProp
                    else:
                        entryDict['relatedVLANNodesDisconnectedProp'] = ''
                    
                    vlansDict.setdefault(vlanID, []).append(entryDict)
        logger.info(f'VLAN IDs retrieved {vlansDict}')  

    except Exception as e:
        logger.error(f'Exception occured while getting a list of distributed port group objects from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}') 
    
    return vlansDict, portGroupSwitchDict
    
def get_distportgroup_property(suite_api_client: SuiteApiClient, distPortGroup: Object, property: str) -> str:
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
        resp_properties_list = json.loads(propResponse.content)["property"]
        
        for respProp in resp_properties_list:
            if respProp["name"] == property:
                return respProp["value"]
    except Exception as e:
        logger.error(f'Exception occured while getting Logical Switch property values from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}')
    return None

def get_vcenter_switch_portgroups(suite_api_client: SuiteApiClient, adapter_instance_id: str, content: Any,):
    logger.info(f'Retrieve port group and their switches from vCenter')
    container = content.rootFolder  # starting point to look into
    view_type = [vim.dvs.DistributedVirtualPortgroup]  # object types to look for
    recursive = True  # whether we should look into it recursively
    container_view = content.viewManager.CreateContainerView(
        container, view_type, recursive
    )
    portGroupSwitchDict = {}
    try:
        children = container_view.view
        for distPortGroup in children:
            portGroupSwitchDict[repr(distPortGroup.config.name).strip("'")] = repr(distPortGroup.config.distributedVirtualSwitch.uuid).strip("'")
            
        logger.info(f'vCenter switch port groups {portGroupSwitchDict}')
    except Exception as e:
        logger.error(f'Exception occured while getting a list of host systems from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}')
    return portGroupSwitchDict    
    
