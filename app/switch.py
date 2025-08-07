#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
from typing import List
from aria.ops.object import Object
from aria.ops.timer import Timer
from aria.ops.suite_api_client import SuiteApiClient
from constants import VCENTER_ADAPTER_KIND
from constants import VCFOPS_DISTSWITCH_UUID_PROPERTY_KEY
import json
import re

logger = logging.getLogger(__name__)


def get_switches(suite_api_client: SuiteApiClient, adapter_instance_id: str):

    switchesByUUID = {}
    
    try:
        logger.info(f'Making VCF Operations REST API call to retrieve a list of Distributed Virtual Switches')
        distSwitchList: List[Object] = suite_api_client.query_for_resources(
            {
                "adapterKind": [VCENTER_ADAPTER_KIND],
                "resourceKind": ["VmwareDistributedVirtualSwitch"],
                "adapterInstanceId": [adapter_instance_id],
            }
        )
        logger.info(f'VCF Operations REST API call returned response with {len(distSwitchList)} Distributed Switch objects')
                   
        for distSwitch in distSwitchList:
            distSwitchUUID = get_distswitch_property(suite_api_client, distSwitch, VCFOPS_DISTSWITCH_UUID_PROPERTY_KEY)
            if distSwitchUUID:
                switchesByUUID[distSwitchUUID] = distSwitch
            else:
                switchesByUUID[distSwitch.get_key().name] = distSwitch
        i = 0
        distSwitchUUIDs = ''
        for key in switchesByUUID:
            i += 1
            distSwitchUUIDs = distSwitchUUIDs + " " + str(i) + "." + key + "\n"
        logger.info(f'Distributed switches retrieved {distSwitchUUIDs}')  
    except Exception as e:
        logger.error(f'Exception occured while getting a list of virtual machines from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}') 
    return switchesByUUID

def get_distswitch_property(suite_api_client: SuiteApiClient, distSwitch: Object, property: str) -> str:
    try:
        logger.info(f'Making VCF Operations REST API call to retrieve distributed switch resource identifier')
        response = suite_api_client.get(f'/api/resources?name={distSwitch.get_key().name}&resourceKind=VmwareDistributedVirtualSwitch&_no_links=true')
        resource_id = json.loads(response.content)["resourceList"][0]["identifier"]
        logger.info(f'Response from VCF Operations REST API call - Distributed switch resource identifier is: {resource_id}')
        if not resource_id:
            logger.info(f'Distributed Switch resource identifier cannot be empty or NULL')
            return None
        
        logger.info(f'Making VCF Operations REST API call to retrieve distributed switch properties')
        propResponse = suite_api_client.get(f'/api/resources/{resource_id}/properties?_no_links=true')
        logger.info(f'Retrieved distributed switch properties from VCF Operations')
        properties_list = json.loads(propResponse.content)["property"]
        for prop in properties_list:
            if prop["name"] == property:
                return prop["value"]
    except Exception as e:
        logger.error(f'Exception occured while getting Logical Switch property values from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}')
    return None


def get_host_switches(host: Object, vSwitchInstanceListCmdOutput: str, parsedENSSwitchList: list, distSwitchesDict: dict):
    vSwitchInstances = []
    hostName = host.get_key().name
    hostToSwitchDict = {}
    hostToSwitchList = []

    with Timer(logger, f'Collection of switch configuration on Host {hostName}'):
        try:
            if vSwitchInstanceListCmdOutput:
                dvsPortSetLines = get_DVSPortSet_lines(vSwitchInstanceListCmdOutput)
                if dvsPortSetLines and len(dvsPortSetLines) > 0:
                    for dvsPortSetLine in dvsPortSetLines:
                        DVSPortRowDict = parse_DVSPortSet_line(dvsPortSetLine)
                        if "vSwitchName" not in DVSPortRowDict or DVSPortRowDict['vSwitchName'] is None or not DVSPortRowDict['vSwitchName']:
                            logger.info(f'Switch name in parsed DVsPortRow of vSwitch instance list command output is empty on host {hostName}. ')
                            return hostToSwitchDict
                        if "friendlyName" not in DVSPortRowDict or DVSPortRowDict['friendlyName'] is None or not DVSPortRowDict['friendlyName']:
                            logger.info(f'Switch label in parsed DVsPortRow of vSwitch instance list command output is empty on host {hostName}. ')
                            return hostToSwitchDict
                        if "switchUUID" not in DVSPortRowDict or DVSPortRowDict['switchUUID'] is None or not DVSPortRowDict['switchUUID']:
                            logger.info(f'Switch UUID in parsed DVsPortRow of vSwitch instance list command output is empty on host {hostName}. ')
                            return hostToSwitchDict
                        vSwitchInstances.append(DVSPortRowDict)
            else:
                logger.info(f'vswitch instance list command output is empty. No switches are configured on host {hostName}. ')
                return hostToSwitchDict
        except Exception as e:
            logger.error(f'Exception occured while parsing DVS Port Set Lines in vSwitch Instance List command output. Exception Type: {type(e).__name__}')
            logger.exception(f'Exception Message: {e}')
            return hostToSwitchDict
        if parsedENSSwitchList and len(parsedENSSwitchList) > 0:
            logger.info(f'parsedENSSwitchList {parsedENSSwitchList}')
            for ensSwitch in parsedENSSwitchList:
                try:
                    if "name" in ensSwitch and ensSwitch['name'] is not None and ensSwitch['name']:
                        ensSwitchName = ensSwitch['name']
                    else:
                        logger.info(f'Switch name not found in the ENS switch list command output on host {hostName}')
                        continue
                    if "swID" in ensSwitch and ensSwitch['swID'] is not None and ensSwitch['swID'] != '':
                        swID = ensSwitch['swID']
                    else:
                        logger.info(f"switch ID not found in the ENS switch list command output on host {hostName}")
                        continue
                    for vSwitchInstance in vSwitchInstances:
                        if vSwitchInstance['vSwitchName'] == ensSwitchName:
                            friendlyName = vSwitchInstance['friendlyName']
                            switchUUID = vSwitchInstance['switchUUID']

                    if friendlyName is None or not friendlyName:
                        logger.info(f'Switch Label is either Null or Empty on host {hostName}')
                        continue

                    if switchUUID is None or not switchUUID:
                        logger.info(f'Switch UUID is either Null or Empty on host {hostName}')
                        continue
                    
                    if switchUUID in distSwitchesDict and distSwitchesDict[switchUUID]:
                        host.add_parent(distSwitchesDict[switchUUID])
                        logger.info(f'Added host {hostName} to switch {friendlyName}-{switchUUID} relationship')
                    switchEntryDict ={}
                    switchEntryDict['switchUUID'] = switchUUID
                    switchEntryDict['switchID'] = swID
                    switchEntryDict['vSwitchName'] = ensSwitchName
                    switchEntryDict['friendlyName'] = friendlyName
                    hostToSwitchList.append(switchEntryDict)
                except Exception as e:
                    logger.error(f'Exception occured while parsing ENS Switch List {parsedENSSwitchList}. Exception Type: {type(e).__name__}')
                    logger.exception(f'Exception Message: {e}')
        else:
            logger.info(f'No ENS switches are configured on host {hostName}')
    hostToSwitchDict[hostName] = hostToSwitchList
    logger.info(f'The following switches are {hostToSwitchDict[hostName]} configured on the host {hostName}.') 
    return hostToSwitchDict


def parse_ensswitch_list(ensSwitchListCmdOutput: str):
    lines = ensSwitchListCmdOutput.strip().splitlines()

    # Skip header and separator
    data_lines = lines[2:]

    ensSwitchList = []
    pattern = re.compile(
        r'^(?P<name>.*?)\s{2,}'           
        r'(?P<swID>\d+)\s+'
        r'(?P<maxPorts>\d+)\s+'
        r'(?P<numActivePorts>\d+)\s+'
        r'(?P<numPorts>\d+)\s+'
        r'(?P<mtu>\d+)\s+'
        r'(?P<numLcores>\d+)\s+'
        r'(?P<lcoreIDs>(?:\d+\s*)+)$'
    )

    for line in data_lines:
        match = pattern.match(line)
        if match:
            groups = match.groupdict()
            entry = {
                'name': groups['name'].strip(),
                'swID': int(groups['swID']),
                'maxPorts': int(groups['maxPorts']),
                'numActivePorts': int(groups['numActivePorts']),
                'numPorts': int(groups['numPorts']),
                'mtu': int(groups['mtu']),
                'numLcores': int(groups['numLcores']),
                'lcoreIDs': groups['lcoreIDs'].strip()
            }
            ensSwitchList.append(entry)
    return ensSwitchList

def parse_DVSPortSet_line(line):
    pattern = r'^(\S+)\s+\(([^)]+)\)\s+(.*)$'
    match = re.match(pattern, line.strip())

    if match:
        entry = {
            'vSwitchName': match.group(1),
            'friendlyName': match.group(2),
            'switchUUID': match.group(3).strip()
        }
        return entry
    else:
        return None

def get_DVSPortSet_lines(vSwitchInstanceListCmdOutput: str):
    dvsPortSetlines = []
    lines = vSwitchInstanceListCmdOutput.strip().split('\n')
    for line in lines:
        if line.strip().startswith("DvsPortset-"):
            dvsPortSetlines.append(line.strip())
    return dvsPortSetlines