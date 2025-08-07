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

def get_nodes(ssh: SSHClient, host: Object, vSwitchInstanceListCmdOutput: str, ensSwitchIDList: list, masterNodeDict: dict):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    nodesDict = {}
    commands = []
    results = []
    hostName = host.get_key().name

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
                results[0] = """NodeIndex Node MAC          vlanID Type Red Box MAC       CurrLcore vDAN MAC          sanA sanB supSeqA supAAge(s) supSeqB supBAge(s) NodeAge(s)
================================================================================================================================================
31        08:00:06:9d:35:9c 204    DAN  bc:e7:12:e4:df:e4 1         00:50:56:96:d3:8a 0    0    2027    0          2027    0          442837  
34        08:00:06:9d:34:f8 201    DAN  34:73:2d:35:48:04 0         00:50:56:96:ae:a0 0    0    39222   0          39222   0          350822  
39        08:00:06:9d:34:eb 201    DAN  34:73:2d:35:48:04 0         00:50:56:96:ae:a0 0    0    39202   1          39202   1          350802  
41        08:00:06:9d:34:fa 201    DAN  34:73:2d:35:48:04 0         00:50:56:96:ae:a0 0    0    39225   0          39225   0          350845  
46        08:00:06:9d:36:aa 207    DAN  6c:13:d5:ab:ef:84 1         00:50:56:96:86:41 0    0    44759   0          44759   0          349229  
52        08:00:06:9d:35:50 203    DAN  bc:e7:12:e4:dc:a4 3         00:50:56:96:df:dd 0    0    31366   0          31366   0          350796  
61        08:00:06:9d:35:59 203    DAN  bc:e7:12:e4:dc:a4 3         00:50:56:96:df:dd 0    0    31372   0          31372   0          350799  
72        08:00:06:9d:35:86 204    DAN  bc:e7:12:e4:df:e4 1         00:50:56:96:d3:8a 0    0    2014    0          2014    0          349317  
74        08:00:06:9d:36:4e 206    DAN  04:5f:b9:cf:1d:a4 3         00:50:56:96:ba:0c 0    0    19966   0          19966   0          449500  
76        08:00:06:9d:36:c7 207    DAN  6c:13:d5:ab:ef:84 1         00:50:56:96:86:41 0    0    44701   0          44701   0          349229      """
                if len(results) == len(commands):
                    for i in range(len(ensSwitchIDList)):
                        try:
                            logger.info(f'Parsing node collection result')
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
                                                if columns[2] is not None and columns[2] != '': 
                                                    lnode.with_property("vlan_id", columns[2].strip())
                                                else:
                                                    lnode.with_property("vlan_id", "")
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
                                                if columns[6] and columns[6].strip():
                                                    lnode.with_property("vdan_mac", str(columns[6].strip()))
                                                else:
                                                    lnode.with_property("vdan_mac", "")
                                                lnode.add_metric(Metric(key="node_age", value=columns[13]))
                                                lnode.add_parent(host)
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
    return nodesDict

def add_node_vlan_relationship(nodesDict: dict, vlansDict: dict, masterHostToSwitchDict: dict) -> None:

    logger.info(f'Starting Node to VLAN relationship creation')
    totalNodeToVLANRelationsAdded = 0
    for node in nodesDict.values():
        vlanID = str(node.get_property('vlan_id')[0].value)
        logger.info(f"Node {node.get_key().name}'s VLAN ID is {vlanID}")
        
        distPortGroupsList = vlansDict.get(vlanID)
        if distPortGroupsList:
            for distPortGroup in distPortGroupsList:
                for hostSwitchList in masterHostToSwitchDict.values():
                    for hostSwitch in hostSwitchList:
                        if distPortGroup['switchUUID'] == hostSwitch['switchUUID'] and distPortGroup['DistPortGroupObject']:
                            node.add_parent(distPortGroup['DistPortGroupObject'])
                            totalNodeToVLANRelationsAdded += 1
                            logger.info(f"Added node {node.get_key().name} to VLAN {vlanID} relationship with distributed port group: {distPortGroup['DistPortGroupObject'].get_key().name}")

    logger.info(f'{totalNodeToVLANRelationsAdded} Node to VLAN relationships were created. ') 

