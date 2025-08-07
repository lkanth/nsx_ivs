#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import re
import json
from typing import List
from aria.ops.timer import Timer
import constants
from aria.ops.object import Identifier
from aria.ops.object import Key
from aria.ops.object import Object
import paramiko
from aria.ops.suite_api_client import SuiteApiClient
from paramiko import SSHClient

logger = logging.getLogger(__name__)

class Port(Object):

    def __init__(self, name: str, uuid: str, host: str):
        """Initializes a Tenant object that represent the ResourceKind defined in line 15 of the describe.xml file.

        :param name: The unique name of used to display the tenant
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
                object_kind="port",
                identifiers=[Identifier(key="uuid", value=uuid), Identifier(key="host", value=host)],
            )
        )


def get_ports(ssh: SSHClient, host: Object, vSwitchInstanceListCmdOutput: str, ensSwitchIDList: list, masterHostToSwitchDict: dict):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    ports = []
    commands = []
    results = []
    hostName = host.get_key().name
    portToHostRelationsAdded = 0

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'Port objects collection on host {hostName}'):
        if vSwitchInstanceListCmdOutput:
            for ensSwitchID in ensSwitchIDList:
                commands.append("nsxdp-cli ens latency system dump -s " + str(ensSwitchID))
            for ensSwitchID in ensSwitchIDList:    
                commands.append("nsxdp-cli ens latency system clear -s " + str(ensSwitchID))
            
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
                        logger.exception(f'Exception Message: {e}')
                    except Exception as e:
                        logger.error(f'Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
                        logger.exception(f'Exception Message: {e}')
                    else:
                        logger.info(f'Command "{command}" output is ({output})')
                hostSwitchList = masterHostToSwitchDict[hostName]
                if len(results) == len(commands): 
                    for i in range(len(ensSwitchIDList)):
                        hostSwitchUUID = None
                        hostSwitchName = None
                        foundHostSwitch = False
                        for hostSwitch in hostSwitchList:
                            if hostSwitch['switchID'] == ensSwitchIDList[i]:
                                hostSwitchName = hostSwitch['friendlyName']
                                hostSwitchUUID = hostSwitch['switchUUID']
                                foundHostSwitch = True
                                break
                        if not foundHostSwitch:
                            logger.info(f'Port switch friendly name not found for switch ID {str(ensSwitchIDList[i])}. Will use UNKNOWN')
                            hostSwitchName = "UNKNOWN"
                            hostSwitchUUID = "UNKNOWN"
                        port_results = re.split("PortID:\s+", results[i])[1:]
                        if port_results and len(port_results) > 0:
                            for port_result in port_results:
                                try:
                                    lines = re.split("\n", port_result)
                                    if len(lines) > 1:
                                        portIDFromCmdOutput = re.split("\s+", lines[0])[0]
                                        if portIDFromCmdOutput is None or portIDFromCmdOutput == '':
                                            logger.info(f'Port ID is either null or empty. Port object not created.')
                                            continue
                                        uuid = portIDFromCmdOutput + "_" + hostName
                                        port = Port(name=portIDFromCmdOutput, uuid=uuid, host=hostName)                                
                                        samples_line = re.split("\s+", lines[2])
                                        min_latency = re.split("\s+", lines[3])
                                        max_latency = re.split("\s+", lines[4])
                                        mean_line = re.split("\s+", lines[5])                                                     

                                        if samples_line[1] is not None and samples_line[1] != '':
                                            port.with_metric("tx_total_samples", samples_line[1])
                                        else:
                                            logger.info(f'tx_total_samples is either null or empty. Port metric tx_total_samples value was not collected.')
                                        if min_latency[1] is not None and min_latency[1] != '':
                                            port.with_metric("tx_min_latency",min_latency[1])
                                        else:
                                            logger.info(f'tx_min_latency is either null or empty. Port metric tx_min_latency value was not collected.')
                                        if max_latency[1] is not None and max_latency[1] != '':
                                            port.with_metric("tx_max_latency",max_latency[1])
                                        else:
                                            logger.info(f'tx_max_latency is either null or empty. Port metric tx_max_latency value was not collected.')
                                        if mean_line[1] is not None and mean_line[1] != '':
                                            port.with_metric("tx_mean",mean_line[1])
                                        else:
                                            logger.info(f'tx_mean is either null or empty. Port metric tx_mean value was not collected.')

                                        if samples_line[2] is not None and samples_line[2] != '':
                                            port.with_metric("rx_total_samples", samples_line[2])
                                        else:
                                            logger.info(f'rx_total_samples is either null or empty. Port metric rx_total_samples value was not collected.')
                                        if min_latency[2] is not None and min_latency[2] != '':
                                            port.with_metric("rx_min_latency",min_latency[2])
                                        else:
                                            logger.info(f'rx_min_latency is either null or empty. Port metric rx_min_latency value was not collected.')
                                        if max_latency[2] is not None and max_latency[2] != '':
                                            port.with_metric("rx_max_latency",max_latency[2])
                                        else:
                                            logger.info(f'rx_max_latency is either null or empty. Port metric rx_max_latency value was not collected.')
                                        if mean_line[2] is not None and mean_line[2] != '':
                                            port.with_metric("rx_mean",mean_line[2])
                                        else:
                                            logger.info(f'rx_mean is either null or empty. Port metric rx_mean value was not collected.')

                                        port.with_property("switch_id",ensSwitchIDList[i])
                                        port.with_property("switch_uuid",hostSwitchUUID)
                                        port.with_property("switch_name",hostSwitchName)
                                        port.with_property("esxi_host", hostName) 
                                        port.add_parent(host)
                                        portToHostRelationsAdded += 1
                                        logger.info(f'Added port {portIDFromCmdOutput} to Host {hostName} relationship on host {hostName}')
                                        ports.append(port)
                                    else:
                                        logger.error(f'Found no metric information for the port: {port_result} on host {hostName}')
                                except Exception as e:
                                    logger.error(f'Exception occured while parsing command output {port_result}. Exception Type: {type(e).__name__}')
                                    logger.exception(f'Exception Message: {e}')
                        else:
                            logger.error(f'Found no metric information for the ports: {port_results} on host {hostName}')
                else:
                    logger.error(f'Number of commands executed does not match with the number of outputs retrieved')
            else:
                logger.info(f'Found zero DvSPortSets')
        else:
            logger.info(f'"nsxdp-cli vswitch instance list command output is empty: "{vSwitchInstanceListCmdOutput}')
    logger.info(f'Collected {len(ports)} ports from host {hostName}')
    logger.info(f'Added host relationships to {portToHostRelationsAdded} Ports on host {hostName}')             
    return ports

def add_port_relationships(vSwitchInstanceListCmdOutput: str, vlansDict: dict, ports: List[Port], vmsByName: dict, suiteAPIClient) -> List:
    RelAddedToVMObjects = []   
    delimiterChar = ".eth"
    vmMacNameDict = {}
    portToVLANRelationsAdded = 0
    with Timer(logger, f'Port to vLAN, Port to VM relationship creation'):
        if ports and len(ports) > 0:
            port_by_name = {}
            for port in ports:
                port_by_name.update({port.get_key().name: port})

            if vSwitchInstanceListCmdOutput:
                portset_results = re.split("DvsPortset", vSwitchInstanceListCmdOutput)
                if portset_results and len(portset_results) > 0:
                    for port_result in portset_results:
                        try:
                            rows = re.split("\n", port_result)[3:]
                            numOfRows = len(rows)                            
                            for rowIndex in range(numOfRows):
                                vSwitchInstanceLineDict =  parse_vSwitch_instance_output(rows[rowIndex]) 
                                if vSwitchInstanceLineDict:
                                    port = port_by_name.get(f"{vSwitchInstanceLineDict['portNumber'].strip()}")                        
                                    distPortGroupsList = vlansDict.get(f"{vSwitchInstanceLineDict['vid'].strip()}")
                                    if not port or not distPortGroupsList:
                                        logger.info(f"Port ({vSwitchInstanceLineDict['portNumber']}:{port}) to VLAN ({str(vSwitchInstanceLineDict['vid'])}:{distPortGroupsList}) relationship was not created.")
                                    else:
                                        portSwitchUUID = port.get_property('switch_uuid')[0].value
                                        for distPortGroup in distPortGroupsList:
                                            if distPortGroup['switchUUID'] == portSwitchUUID and distPortGroup['DistPortGroupObject']:
                                                port.add_parent(distPortGroup['DistPortGroupObject'])
                                                portToVLANRelationsAdded += 1
                                                logger.info(f"Port {port.get_key().name} to VLAN {vSwitchInstanceLineDict['vid'].strip()} relationship was created with distributed port group: {distPortGroup['DistPortGroupObject'].get_key().name}")
                                        port.with_property("vlan_id", vSwitchInstanceLineDict['vid'].strip()) 
                                    
                                    vmNICMacaddress = vSwitchInstanceLineDict['macAddress'].strip()
                                    clientName = vSwitchInstanceLineDict['clientName'].strip()
                                    subRowIndex = rowIndex
                                    while ((subRowIndex + 1) < numOfRows):                                
                                        subRowIndex += 1
                                        nextvSwitchInstanceLineDict =  parse_vSwitch_instance_output(rows[subRowIndex])                                     
                                        if nextvSwitchInstanceLineDict is None or not nextvSwitchInstanceLineDict:                              
                                            if parse_vSwitch_instance_output_noport(rows[subRowIndex]):
                                                clientName = clientName + rows[subRowIndex].strip()
                                            else:                                    
                                                break
                                        else:
                                            break                             
                                                            
                                    vmName = ""
                                    lastIndex = clientName.rfind(delimiterChar)
                                    if lastIndex != -1:
                                        vmName = clientName[:lastIndex]
                                        vmMacNameDict[vmNICMacaddress] = vmName                                                  
                                        vms = vmsByName.get(vmName)
                                        if not vms or not port:
                                            logger.info(f"Port ({vSwitchInstanceLineDict['portNumber']}:{port}) to VM({vmName}:{vms}) relationship was not created.")
                                        else:                       
                                            port.with_property("vm", vmName)
                                            if len(vms) == 1:                                        
                                                port.add_parent(vms[0])
                                                logger.info(f"Port ({port.get_key().name}) to VM ({vms[0].get_key().name}) relationship was created")
                                                RelAddedToVMObjects.append(vms[0])                                        
                                            elif len(vms) > 1:
                                                vmMOID = get_vm_moid(suiteAPIClient,vmName,vmNICMacaddress)
                                                for vm in vms:                                            
                                                    if vm.get_identifier_value("VMEntityObjectID") == vmMOID:                                                
                                                        port.add_parent(vm)
                                                        logger.info(f"Port ({port.get_key().name}) to VM ({vm.get_key().name}) relationship was created")
                                                        RelAddedToVMObjects.append(vm)
                                            else:
                                                logger.info(f"Port ({vSwitchInstanceLineDict['portNumber']}:{port}) to VM({vmName}:{vms}) relationship was not created.")
                                    else:
                                        logger.info(f"Client name does not end with .eth<Number> Port ({vSwitchInstanceLineDict['portNumber']}:{port}) to VM relationship was not created.")
                                else:
                                    logger.info(f'Not a valid port line {rows[rowIndex]}')
                        except Exception as e:
                            logger.error(f'Exception occured while creating VM and VLAN relationship to ports. Exception Type: {type(e).__name__}')
                            logger.exception(f'Exception Message: {e}')
                else:
                    logger.info(f'Found zero DvSPortSets')
            else:
                logger.info(f'"nsxdp-cli vswitch instance list command output is empty: "{vSwitchInstanceListCmdOutput}')
        else:
            logger.info(f'No relations to ports can be added. Port list is empty - ({ports})')
        
    logger.info(f'Added VM relationships to {len(RelAddedToVMObjects)} Ports')
    logger.info(f'Added VLAN relationships to {portToVLANRelationsAdded} Ports')        
    return RelAddedToVMObjects, vmMacNameDict

def get_vm_moid(suite_api_client: SuiteApiClient, vmName: str, vmNICMacaddress: str ) -> str:
    try:
        vmResourceIDs = []
        MAC_ADDRESS_PROPERTY_NAME = "mac_address"
        VM_MOID_PROPERTY_NAME = "summary|MOID"
        vmMOID = ""
        vmResourceIDsStr=""
        logger.info(f'Making VCF Operations REST API call to retrieve VM resource identifier')
        vmsResponse = suite_api_client.get(f'/api/resources?name={vmName}&adapterKind=VMWARE&resourceKind=VirtualMachine&_no_links=true')
        
        vmResourceList = json.loads(vmsResponse.content)["resourceList"]
        for vmResource in vmResourceList:
            vmResourceIDs.append(vmResource["identifier"]) 
        for vmResourceID in vmResourceIDs:
            vmResourceIDsStr = vmResourceIDsStr + "resourceId=" + vmResourceID + "&"

        logger.info(f'Response from VCF Operations REST API call - VM Resource ID: {vmResourceIDsStr}')
        logger.info(f'Making VCF Operations REST API call to retrieve VM properties')
        propResponse = suite_api_client.get(f'/api/resources/properties?{vmResourceIDsStr}_no_links=true')
        logger.info(f'Retrieved VM properties from VCF Operations')
        resourcePropertiesList = json.loads(propResponse.content)["resourcePropertiesList"]
        for resourceProperty in resourcePropertiesList:            
            propList = resourceProperty["property"]
            for prop in propList:
                if MAC_ADDRESS_PROPERTY_NAME in prop["name"]:
                    if vmNICMacaddress == prop["value"]:
                        for propID in propList:
                            if propID["name"] == VM_MOID_PROPERTY_NAME:
                                vmMOID = propID["value"]
                                return vmMOID                  
    except Exception as e:
        logger.error(f'Exception occured while getting VM MOID from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}')
    return None


def parse_vSwitch_instance_output(line):
    pattern = r"(^.*) +(\d{5,}) +([0-9a-f-]+) +([0-9a-f:]+) +(\S+) +(\S+) +(\S+)"
    match = re.match(pattern, line)
    if match:
        clientName, portNumber, DVPortID, macAddress, uplink, vid, vni = match.groups()
        return {
            "clientName": clientName,
            "portNumber": portNumber,
            "DVPortID": DVPortID,
            "macAddress": macAddress,
            "uplink": uplink,
            "vid": vid,
            "vni": vni,
        }
    else:
        return None
    
def parse_vSwitch_instance_output_noport(line):
    pattern = r"^(?!.*\b\d{8,}\b).*$"
    match = re.match(pattern, line)
    if match:
        return True
    else:
        return False