#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import re
import json
from aria.ops.timer import Timer
import constants
from aria.ops.data import Metric
from aria.ops.object import Identifier
from aria.ops.object import Key
from aria.ops.object import Object
import paramiko
from paramiko import SSHClient

logger = logging.getLogger(__name__)

class Node(Object):

    def __init__(self, name: str, uuid: str):
        """Initializes a Tenant object that represent the ResourceKind defined in line 15 of the describe.xml file.

        :param name: The  unique name of used to display the tenant
        :param uuid: A Universal Unique Identifier for the Tenant
        :param url: A URL with the AVI controller, and the tenant's UUID
        """
        self.uuid = uuid
        self.name = name
        super().__init__(
            key=Key(
                name=name,
                # adapter_kind should match the key defined for the AdapterKind in line 4 of the describe.xml
                adapter_kind=constants.ADAPTER_KIND,
                # object_kind should match the key used for the ResourceKind in line 15 of the describe.xml
                object_kind="node",
                identifiers=[Identifier(key="uuid", value=uuid)],
            )
        )

def get_nodes(ssh: SSHClient, host: Object, vSwitchInstanceListCmdOutput: str, ensSwitchIDList: list, masterNodeDict: dict, vlansDict: dict, masterHostToSwitchDict: dict, hostVDANSList: list):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    nodesDict = {}
    commands = []
    results = []
    hostName = host.get_key().name
    totalNodeToVLANRelationsAdded = 0
    totalNodeToVDANRelationsAdded = 0
    hostSwitchList = masterHostToSwitchDict[hostName]

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'Node object collection on host {hostName}'):
        if vSwitchInstanceListCmdOutput:
            for ensSwitchID in ensSwitchIDList:
                commands.append("nsxdp-cli ens prp node list -s " + str(ensSwitchID))

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
                            return nodesDict
                    except paramiko.AuthenticationException as e:
                        logger.error(f'Authentication failed, please verify your credentials. Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
                        logger.exception(f'Exception Message: {e}')
                        return nodesDict
                    except paramiko.SSHException as e:
                        logger.error(f'SSH error occurred. Exception Type: {type(e).__name__}')
                        logger.exception(f'Exception Message: {e}')
                        return nodesDict
                    except Exception as e:
                        logger.error(f'Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
                        logger.exception(f'Exception Message: {e}')
                        return nodesDict
                    else:
                        logger.info(f'Node collection command output "{output}"')
                if len(results) == len(commands):
                    for i in range(len(ensSwitchIDList)):
                        try:
                            logger.info(f'Parsing node collection result')
                            hostSwitchUUID = None
                            foundHostSwitch = False
                            for hostSwitch in hostSwitchList:
                                logger.info(f"Host switch ID: {hostSwitch['switchID']} - Node switch ID {str(ensSwitchIDList[i])}.")
                                if hostSwitch['switchID'] == ensSwitchIDList[i]:
                                    hostSwitchUUID = hostSwitch['switchUUID']
                                    foundHostSwitch = True
                                    break
                            if not foundHostSwitch:
                                logger.info(f'Node switch UUID not found for switch ID {str(ensSwitchIDList[i])}. Will use UNKNOWN')
                                hostSwitchUUID = "UNKNOWN"
                            if results[i]:
                                node_results = re.split("\n", results[i])
                                j = 2
                                while j < len(node_results):
                                    columns = re.split("\s+", node_results[j])
                                    if columns[0] is not None and columns[0] != '' and columns[0].isnumeric():
                                        if columns[1] and columns[1].strip(): 
                                            mac = columns[1].strip()
                                            if mac in masterNodeDict and masterNodeDict[mac]:
                                                masterNodeDict[mac].add_parent(host)
                                                j += 1
                                                continue
                                            else:
                                                lnode = Node(name = mac, uuid = mac)
                                                lnode.with_property("mac", mac)
                                                nodeVLANID = ""
                                                if columns[2] is not None and columns[2] != '': 
                                                    nodeVLANID = columns[2].strip()
                                                    lnode.with_property("vlan_id", nodeVLANID)
                                                else:
                                                    lnode.with_property("vlan_id", nodeVLANID)
                                                if columns[3] and columns[3].strip():
                                                    lnode.with_property("type", columns[3].strip())
                                                else:
                                                    lnode.with_property("type", "")
                                                if columns[4] and columns[4].strip():
                                                    lnode.with_property("redbox_mac", columns[4].strip())
                                                else:
                                                    lnode.with_property("redbox_mac", "")
                                                if columns[5] is not None and columns[5] != '': 
                                                    lnode.with_property("current_lcore", str(columns[5].strip()))
                                                else:
                                                    lnode.with_property("current_lcore", "")
                                                nodeVDANMac = ""
                                                if columns[6] and columns[6].strip():
                                                    nodeVDANMac = str(columns[6].strip())
                                                    lnode.with_property("vdan_mac", nodeVDANMac)
                                                else:
                                                    lnode.with_property("vdan_mac", nodeVDANMac)
                                                if columns[9] is not None and columns[9] != '' and columns[9].isnumeric(): 
                                                    lnode.with_metric("sup_seq_a",int(columns[9]))
                                                else:
                                                    lnode.with_metric("sup_seq_a",0)
                                                if columns[11] is not None and columns[11] != '' and columns[11].isnumeric(): 
                                                    lnode.with_metric("sup_seq_b",int(columns[11]))
                                                else:
                                                    lnode.with_metric("sup_seq_b",0)
                                                if columns[13] is not None and columns[13] != '' and columns[13].isnumeric(): 
                                                    lnode.with_metric("node_age",int(columns[13]))
                                                else:
                                                    lnode.with_metric("node_age",0)
                                                lnode.add_parent(host)
                                                if nodeVLANID is not None and nodeVLANID != '':
                                                    if add_node_vlan_relationship(lnode, nodeVLANID, hostSwitchUUID, vlansDict):
                                                        totalNodeToVLANRelationsAdded += 1
                                                else:
                                                     logger.info(f'VLAN ID is null or empty. Node {str(columns[1])} to VLAN relationship was not created.')
                                                if nodeVDANMac:
                                                    if add_node_vdan_relationship(lnode, nodeVDANMac, hostVDANSList):
                                                        totalNodeToVDANRelationsAdded += 1
                                                else:
                                                     logger.info(f'VDAN Mac is null or empty. Node {str(columns[1])} to VDAN relationship was not created.')
                                                logger.info(f'Node MAC {str(columns[1])} VLAN ID {columns[2]} Type {columns[3]} Redbox Mac {columns[4]} Current LCORE {columns[4]}')
                                                logger.info(f'Added Node {str(columns[1])} to Host {hostName} relationship')
                                                nodesDict[mac] = lnode
                                                j += 1
                                        else:
                                            j += 1
                                    else:
                                        j += 1
                            else:
                                logger.error(f'Node list command output is empty or NULL')
                        except Exception as e:
                            logger.error(f'Exception occured while parsing command output "{results[i]}". Exception Type: {type(e).__name__}')
                            logger.exception(f'Exception Message: {e}')
                else:
                    logger.error(f'Number of commands executed does not match with the number of outputs retrieved')
            else:
                logger.info(f'No commands to run to gather node objects')
        else:
            logger.info(f'"nsxdp-cli vswitch instance list command output is empty: "{vSwitchInstanceListCmdOutput}')
    logger.info(f'Collected {len(nodesDict)} nodes from host {hostName}')
    logger.info(f'{totalNodeToVLANRelationsAdded} Node to VLAN relationships were created on host {hostName} ')
    logger.info(f'{totalNodeToVDANRelationsAdded} Node to VDAN relationships were created on host {hostName} ')  
    return nodesDict

def add_node_vlan_relationship(node: Object, nodeVLANID, hostSwitchUUID: str, vlansDict: dict):
    logger.info(f'Starting Node to VLAN relationship creation')
    if nodeVLANID.isnumeric():
        nodeVLANID = str(nodeVLANID)
    distPortGroupsList = vlansDict.get(nodeVLANID)
    if distPortGroupsList:
        for distPortGroup in distPortGroupsList:
            if distPortGroup['switchUUID'] == hostSwitchUUID and distPortGroup['DistPortGroupObject']:
                node.add_parent(distPortGroup['DistPortGroupObject'])
                distPortGroup['currentNumRelatedNodes'] = distPortGroup['currentNumRelatedNodes'] + 1
                logger.info(f"Added node {node.get_key().name} to VLAN {nodeVLANID} relationship with distributed port group: {distPortGroup['DistPortGroupObject'].get_key().name}")
                return True
    return False

def add_node_vdan_relationship(node: Object, nodeVDANMac: str, hostVDANSList: list):
    logger.info(f'Starting VDAN to Node relationship creation')
    for vdanObj in hostVDANSList:
        vDANMacAddress = str(vdanObj.get_property('mac_address')[0].value)
        if vDANMacAddress == nodeVDANMac:
            node.add_parent(vdanObj)
            logger.info(f"Added VDAN {vdanObj.get_key().name} to Node {node.get_key().name} relationship")
            return True
    return False
    
    

