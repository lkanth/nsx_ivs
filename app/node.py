#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import re
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

    def __init__(self, name: str, uuid: str, host):
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
                object_kind="node",
                identifiers=[Identifier(key="uuid", value=uuid)],
            )
        )

def get_nodes(ssh: SSHClient, host: Object):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    nodes = []
    command = "nsxdp-cli ens prp stats node list"
    hostName = host.get_key().name

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'Node object collection on host {hostName}'):
        try:
            logger.info(f'Running command "{command}" on host {hostName}')
            stdin, stdout, stderr = ssh.exec_command(command, timeout=constants.SSH_COMMAND_TIMEOUT)
            error = stderr.read().decode().strip()
            result = stdout.read().decode()
            exit_status = stdout.channel.recv_exit_status()
            if exit_status == 0:
                logger.info(f'Successfully ran the command "{command}" on host {hostName}')
            else:
                logger.error(f'Command failed with exit status {exit_status}. Error: {error}')
                return nodes
        except paramiko.AuthenticationException as e:
            logger.error(f'Authentication failed, please verify your credentials. Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
            logger.exception(f'Exception Message: {e}')
            return nodes
        except paramiko.SSHException as e:
            logger.error(f'SSH error occurred. Exception Type: {type(e).__name__}')
            logger.exception(f'Exception Message: {e}')
            return nodes
        except Exception as e:
            logger.error(f'Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
            logger.exception(f'Exception Message: {e}')
            return nodes
        else:
            logger.info(f'Node collection command output "{result}"')
        try:
            logger.info(f'Parsing node collection result')
            if result:
                node_results = re.split("\n", result)
                i = 2
                while i < len(node_results):
                    columns = re.split("\s+", node_results[i])
                    if columns[0].isnumeric():
                        mac = columns[1].strip()
                        lnode = Node(name = mac, uuid = mac)
                        lnode.with_property("mac", mac)
                        lnode.with_property("vlan_id", columns[2].strip())
                        lnode.with_property("type", columns[3])
                        lnode.add_metric(Metric(key="node_age", value=columns[4]))
                        lnode.add_parent(host)
                        logger.info(f'Added Node {str(columns[0])} to Host {hostName} relationship')
                        nodes.append(lnode)
                        i+=1
                    else:
                        i += 1
            else:
                logger.error(f'Node list command output is empty or NULL')
                return nodes
        except Exception as e:
            logger.error(f'Exception occured while parsing command output "{result}". Exception Type: {type(e).__name__}')
            logger.exception(f'Exception Message: {e}')
    logger.info(f'Collected {len(nodes)} nodes from host {hostName}') 
    return nodes

def add_node_vlan_relationship(hostName: str, nodes: list, vlansDict: dict, portGroupSwitchDict: dict, masterHostToSwitchDict: dict) -> None:

    logger.info(f'Starting Node to VLAN relationship creation on host {hostName}')
    hostSwitchList = masterHostToSwitchDict[hostName]
    totalNodeToVLANRelationsAdded = 0
    for node in nodes:
        vlanID = str(node.get_property('vlan_id')[0].value)
        logger.info(f"Node {node.get_key().name}'s VLAN ID is {vlanID}")
        
        distPortGroupsList = vlansDict.get(vlanID)
        if distPortGroupsList:
            for distPortGroup in distPortGroupsList:
                for hostSwitch in hostSwitchList:
                    if distPortGroup['switchUUID'] == hostSwitch['switchUUID'] and distPortGroup['DistPortGroupObject']:
                        node.add_parent(distPortGroup['DistPortGroupObject'])
                        totalNodeToVLANRelationsAdded += 1
                        logger.info(f"Added node {node.get_key().name} to VLAN {vlanID} relationship with distributed port group: {distPortGroup['DistPortGroupObject'].get_key().name}")

    logger.info(f'{totalNodeToVLANRelationsAdded} Node to VLAN relationships on host {hostName} were created. ') 

