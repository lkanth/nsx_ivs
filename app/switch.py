#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
from typing import List

import constants
from aria.ops.data import Metric
from aria.ops.object import Identifier
from aria.ops.object import Key
from aria.ops.object import Object
from aria.ops.timer import Timer

import re

logger = logging.getLogger(__name__)


class Switch(Object):

    def __init__(self, name: str, uuid: str, host: str):
        self.uuid = uuid
        self.name = name
        self.host = host
        super().__init__(
            key=Key(
                name=name,
                # adapter_kind should match the key defined for the AdapterKind in line 4 of the describe.xml
                adapter_kind=constants.ADAPTER_KIND,
                # object_kind should match the key used for the ResourceKind in line 15 of the describe.xml
                object_kind="switch",
                identifiers=[Identifier(key="uuid", value=uuid), Identifier(key="host", value=host)],
            )
        )


def get_switches(host: Object, parsedENSSwitchList: List, vSwitchInstanceListCmdOutput: str, masterSwitchList: List) -> List[Switch]:

    switches = []
    vSwitchInstances = []
    hostName = host.get_key().name
    hostToSwitchRelationsAdded = 0

    with Timer(logger, f'Switch objects collection on host {hostName}'):
        try:
            if vSwitchInstanceListCmdOutput is not None and vSwitchInstanceListCmdOutput:
                dvsPortSetLines = getDVSPortSetLines(vSwitchInstanceListCmdOutput)
                if dvsPortSetLines is not None and dvsPortSetLines and len(dvsPortSetLines) > 0:
                    for dvsPortSetLine in dvsPortSetLines:
                        DVSPortRowDict = parseDVSPortSetLine(dvsPortSetLine)
                        if "vSwitchName" not in DVSPortRowDict or DVSPortRowDict['vSwitchName'] is None or not DVSPortRowDict['vSwitchName']:
                            logger.info(f'Switch name in parsed DVsPortRow of vSwitch instance list command output is empty on host {hostName}. ')
                            return switches
                        if "friendlyName" not in DVSPortRowDict or DVSPortRowDict['friendlyName'] is None or not DVSPortRowDict['friendlyName']:
                            logger.info(f'Switch label in parsed DVsPortRow of vSwitch instance list command output is empty on host {hostName}. ')
                            return switches
                        if "switchUUID" not in DVSPortRowDict or DVSPortRowDict['switchUUID'] is None or not DVSPortRowDict['switchUUID']:
                            logger.info(f'Switch UUID in parsed DVsPortRow of vSwitch instance list command output is empty on host {hostName}. ')
                            return switches
                        vSwitchInstances.append(DVSPortRowDict)
            else:
                logger.info(f'vswitch instance list command output is empty. No switches are configured on host {hostName}. ')
                return switches
        except Exception as e:
            logger.error(f'Exception occured while parsing DVS Port Set Lines in vSwitch Instance List command output. Exception Type: {type(e).__name__}')
            logger.exception(f'Exception Message: {e}')
            return switches
        if parsedENSSwitchList is not None and parsedENSSwitchList and len(parsedENSSwitchList) > 0:
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
                    if "maxPorts" in ensSwitch and ensSwitch['maxPorts'] is not None and ensSwitch['maxPorts'] != '':
                        maxPorts = ensSwitch['maxPorts']
                    else:
                        logger.info(f'Max ports not found in the ENS switch list command output on host {hostName}')
                        continue
                    if "mtu" in ensSwitch and ensSwitch['mtu'] is not None and ensSwitch['mtu'] != '':
                        mtu = ensSwitch['mtu']
                    else:
                        logger.info(f'MTU not found in the ENS switch list command output on host {hostName}')
                        continue
                    if "numLcores" in ensSwitch and ensSwitch['numLcores'] is not None and ensSwitch['numLcores'] != '':
                        numLcores = ensSwitch['numLcores']
                    else:
                        logger.info(f'Number of LCores not found in the ENS switch list command output on host {hostName}')
                        continue
                    if "lcoreIDs" in ensSwitch and ensSwitch['lcoreIDs'] is not None and ensSwitch['lcoreIDs'] != '':
                        lcoreIDs = ensSwitch['lcoreIDs']
                    else:
                        logger.info(f'lcoreIDs not found in the ENS switch list command output on host {hostName}')
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
                    
                    foundMasterSwitch = False
                    if masterSwitchList is not None and masterSwitchList and len(masterSwitchList) > 0:
                        for masterSwitch in masterSwitchList:
                            if masterSwitch.get_property_values("switch_uuid")[0] == switchUUID:
                                host.add_parent(masterSwitch)
                                logger.info(f'Added host {hostName} to switch {swID} relationship')
                                hostToSwitchRelationsAdded += 1
                                foundMasterSwitch = True
                                break
                    else:
                        logger.info(f'No switches in the collection yet.')
                        foundMasterSwitch = False
                    if not foundMasterSwitch:
                        uuid = str(swID) + "_" + switchUUID + "_" + hostName
                        switch = Switch(name=friendlyName, uuid=uuid, host=hostName)
                        switch.with_property("switch_id", swID)
                        switch.with_property("esxi_host", hostName)
                        switch.with_property("internal_name", ensSwitchName)
                        switch.with_property("switch_uuid", switchUUID)
                        switch.with_property("max_ports", maxPorts)
                        switch.with_property("MTU", mtu)
                        switch.with_property("num_lcores", numLcores)
                        switch.with_property("lcore_ids", lcoreIDs)

                        host.add_parent(switch)
                        hostToSwitchRelationsAdded += 1
                        logger.info(f'Added host {hostName} to switch {swID} relationship')
                        switches.append(switch)
                        logger.info(f'Added one switch to the collection.')
                    else:
                        logger.info(f'Switch is not different from the ones collected already')
                except Exception as e:
                    logger.error(f'Exception occured while parsing ENS Switch List {parsedENSSwitchList}. Exception Type: {type(e).__name__}')
                    logger.exception(f'Exception Message: {e}')
        else:
            logger.info(f'No ENS switches are configured on host {hostName}')
    logger.info(f'There are {len(masterSwitchList)} switches that are already connected to the host {hostName}. Collected {len(switches)} additional switches from host {hostName}. ')
    logger.info(f'Added Host relationships to {hostToSwitchRelationsAdded} switches')  
    return switches



def parseENSSwitchList(ensSwitchListCmdOutput: str):
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

def parseDVSPortSetLine(line):
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

def getDVSPortSetLines(vSwitchInstanceListCmdOutput: str):
    dvsPortSetlines = []
    lines = vSwitchInstanceListCmdOutput.strip().split('\n')
    for line in lines:
        if line.strip().startswith("DvsPortset-"):
            dvsPortSetlines.append(line.strip())
    return dvsPortSetlines