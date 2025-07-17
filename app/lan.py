#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import re
import json
from typing import List
from typing import Any
import traceback
from aria.ops.timer import Timer
import constants
from aria.ops.data import Metric
from aria.ops.object import Identifier
from aria.ops.object import Key
from aria.ops.object import Object
import paramiko
from aria.ops.suite_api_client import SuiteApiClient
from paramiko import SSHClient
from datetime import datetime


logger = logging.getLogger(__name__)


class Lan(Object):

    def __init__(self, name: str, uuid: str, host: str, switchID: str):
        """Initializes a Tenant object that represent the ResourceKind defined in line 15 of the describe.xml file.

        :param name: The  unique name of used to display the tenant
        :param uuid: A Universal Unique Identifier for the Tenant
        :param url: A URL with the AVI controller, and the tenant's UUID
        """
        self.uuid = uuid
        self.name = name
        self.host = host
        super().__init__(
            key=Key(
                name=name,
                # adapter_kind should match the key defined for the AdapterKind in line 4 of the describe.xml
                adapter_kind=constants.ADAPTER_KIND,
                # object_kind should match the key used for the ResourceKind in line 15 of the describe.xml
                object_kind="lan",
                identifiers=[Identifier(key="uuid", value=uuid), Identifier(key="host", value=host), Identifier(key="switchID", value=switchID)],
            )
        )


def get_lans(ssh: SSHClient, host: Object, vSwitchInstanceListCmdOutput: str, ensSwitchIDList: List, switches: List):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    hostName = host.get_key().name
    lanObjectList = []
    commands = []
    results = []
    lanToSwitchRelationsAdded = 0

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'LAN objects collection on {hostName}'):
        if vSwitchInstanceListCmdOutput is not None and vSwitchInstanceListCmdOutput:
            for ensSwitchID in ensSwitchIDList:
                commands.append("nsxcli -c get ens prp config " + str(ensSwitchID))
        numOfCommands = len(commands)
        if numOfCommands > 0:
            for command in commands:
                try:
                    logger.info(f'Running command "{command}" on host {hostName}')
                    stdin, stdout, stderr = ssh.exec_command(command, timeout=constants.SSH_COMMAND_TIMEOUT)
                    error = stderr.read().decode().strip()
                    output = stdout.read().decode()
                    results.append(output)
                    exit_status = stdout.channel.recv_exit_status()
                    if exit_status == 0:
                        logger.info(f'Successfully ran the command "{command}" on host {hostName}')
                    else:
                        logger.error(f'Command failed with exit status {exit_status}. Error: {error}')
                except paramiko.AuthenticationException as e:
                    logger.error(f'Authentication failed, please verify your credentials. Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
                    logger.exception(f'Exception Message: {e}')
                except paramiko.SSHException as e:
                    logger.error(f'SSH error occurred. Exception Type: {type(e).__name__}')
                    logger.exception(f"Exception Message: {e}")
                except Exception as e:
                    logger.error(f'Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
                    logger.exception(f'Exception Message: {e}')
                else:
                    logger.info(f'Command "{command}" output is ({output})')

            
            if len(results) == len(commands):
                for i in range(len(results)):
                    try:                   
                        parsed_lan_output = parse_lan_output(results[i])
                        lanList = []
                        for key in parsed_lan_output:
                            if key.startswith("Lan"):
                                lanList.append(key)
                        if len(lanList) > 0:
                            if "switchID" in parsed_lan_output and parsed_lan_output['switchID'] is not None and parsed_lan_output['switchID'] != '':
                                switchID = parsed_lan_output['switchID']
                            else:
                                logger.info(f"switch ID not found in the LAN list (get ens prp config) command output on host {hostName}")
                                continue
                            switchIDStr = str(switchID)
                            for lanItem in lanList:
                                if "status" in parsed_lan_output[lanItem]:                                
                                    uuid = lanItem + "_" + hostName + "_switch_" + switchIDStr
                                    lan = Lan(name=lanItem, uuid=uuid, host=hostName, switchID=switchIDStr)                               
                                    lan.with_property("esxi_host", hostName)
                                    lan.with_property("switch_id", switchID)
                                    if "status" in parsed_lan_output[lanItem] and parsed_lan_output[lanItem]['status'] is not None and parsed_lan_output[lanItem]['status']:
                                        lan.with_property("status", parsed_lan_output[lanItem]["status"])
                                    else:
                                        logger.info(f'status is either null or empty. LAN metric status value was not collected.')
                                    if "uplink1" in parsed_lan_output[lanItem] and parsed_lan_output[lanItem]['uplink1'] is not None and parsed_lan_output[lanItem]['uplink1']:
                                        lan.with_property("uplink1", value=parsed_lan_output[lanItem]["uplink1"])
                                    else:
                                        logger.info(f'uplink1 is either null or empty. LAN metric uplink1 value was not collected.')
                                    if "uplink2" in parsed_lan_output[lanItem] and parsed_lan_output[lanItem]['uplink2'] is not None and parsed_lan_output[lanItem]['uplink2']:
                                        lan.with_property("uplink2", value=parsed_lan_output[lanItem]["uplink2"])
                                    else:
                                        logger.info(f'uplink2 is either null or empty. LAN metric uplink2 value was not collected.')
                                    if "policy" in parsed_lan_output[lanItem] and parsed_lan_output[lanItem]['policy'] is not None and parsed_lan_output[lanItem]['policy']:
                                        lan.with_property("policy", value=parsed_lan_output[lanItem]["policy"])
                                    else:
                                        logger.info(f'policy is either null or empty. LAN metric policy value was not collected.')
                                    
                                    addedLANToSwitchRelationShip = False
                                    for switch in switches:
                                        switchID = switch.get_property_values("switch_id")[0]
                                        switchName = switch.get_key().name
                                        if switchID == ensSwitchIDList[i]:
                                            lan.with_property("switch_id",switchID)
                                            lan.with_property("switch_name",switchName)
                                            lan.add_parent(switch)
                                            addedLANToSwitchRelationShip = True
                                            logger.info(f'Added LAN {lanItem} to Switch {switchID} relationship on host {hostName}')
                                            lanToSwitchRelationsAdded += 1
                                            break
                                    if not addedLANToSwitchRelationShip:
                                        logger.info(f'LAN {lanItem} to Switch {ensSwitchIDList} relationship was not created on host {hostName}')

                                    lanObjectList.append(lan)
                        else:
                            logger.error(f'List of Lans is empty in the parsed lan output: {parsed_lan_output}') 
                    except Exception as e:
                        logger.error(f'Exception occured while parsing command output {results[i]}. Exception Type: {type(e).__name__}')
                        logger.exception(f'Exception Message: {e}')
            else:
                logger.error(f'Number of commands executed does not match with the number of outputs retrieved')
        else:
            logger.info(f'Found zero DvSPortSets')
    logger.info(f'Collected {len(lanObjectList)} Lan objects from host {hostName}') 
    return lanObjectList


def parse_lan_output(output):
    result = {}

    date_line = re.search(r'^[A-Z][a-z]{2} [A-Z][a-z]{2} \d{1,2} \d{4} UTC \d{2}:\d{2}:\d{2}\.\d{3}', output, re.MULTILINE)
    if date_line:
        result['timestamp'] = date_line.group()

    switch_match = re.search(r'PRP Config for switch\s+(\d+)', output)
    if switch_match:
        result['switchID'] = int(switch_match.group(1))

    uplink_match = re.search(r'PRP uplink\(channel\)\s+(\S+)', output)
    if uplink_match:
        result['uplink_channel'] = uplink_match.group(1)

    lan_names = re.findall(r'^\s*(Lan\w+)', output, re.MULTILINE)
    distinct_lan_names = sorted(set(lan_names))

    for lan in distinct_lan_names:
        lan_info = {}
        for field in ['uplink1', 'uplink2', 'policy', 'status']:
            match = re.search(rf'{lan} {field}\s+(\S+)', output)
            if match:
                lan_info[field] = match.group(1)
        result[lan] = lan_info

    mac_match = re.search(r'Red Box MAC\s+([0-9a-f:]+)', output, re.IGNORECASE)
    if mac_match:
        result['red_box_mac'] = mac_match.group(1)

    sm_match = re.search(r'Default Supervision Multicast\s+([0-9a-f:]+)', output, re.IGNORECASE)
    if sm_match:
        result['default_supervision_multicast'] = sm_match.group(1)

    return result