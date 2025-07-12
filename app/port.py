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
                identifiers=[Identifier(key="port", value=uuid), Identifier(key="host", value=host)],
            )
        )


def get_ports(ssh: SSHClient, host: Object, vSwitchInstanceListCmdOutput: str):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    uuidPorts = []
    prpDvsPortsetNumbers = []
    ports = []
    commands = []
    results = []
    hostName = host.get_key().name

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'Port objects collection on host {hostName}'):
        if vSwitchInstanceListCmdOutput is not None and vSwitchInstanceListCmdOutput:
            prpDvsPortsetNumbers = re.findall(r'^DvsPortset-(\d+)\s*\(.*', vSwitchInstanceListCmdOutput, re.MULTILINE)
            for prpDvsPortNumber in prpDvsPortsetNumbers:
                commands.append("nsxdp-cli ens latency system dump -s " + str(prpDvsPortNumber))
            for prpDvsPortNumber in prpDvsPortsetNumbers:    
                commands.append("nsxdp-cli ens latency system clear -s " + str(prpDvsPortNumber))
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

                if len(results) == len(commands) :                 
                    try:
                        portset_results = re.split("DvsPortset", vSwitchInstanceListCmdOutput)
                        for port_result in portset_results:
                            rows = re.split("\n", port_result)[3:]
                            numOfRows = len(rows)                            
                            for rowIndex in range(numOfRows):
                                columns = re.split("\s+", rows[rowIndex])        
                                if(len(columns) > 3 and columns[3].strip().count('-') == 4): 
                                    portWithUUID = columns[2].strip()
                                    uuidPorts.append(portWithUUID)
                    except Exception as e:
                        logger.error(f'Error processing ssh command results: {e}') 
                    
                    for i in range(len(prpDvsPortsetNumbers)):
                        port_results = re.split("PortID:\s+", results[i])[1:]      
                        for port_result in port_results:
                            try:
                                lines = re.split("\n", port_result)
                                if len(lines) > 1:
                                    portIDFromCmdOutput = re.split("\s+", lines[0])[0]
                                    for uuidPortID in uuidPorts:
                                        if uuidPortID == portIDFromCmdOutput.strip():
                                            uuid = uuidPortID + "_" + hostName
                                            port = Port(name=uuidPortID, uuid=uuid, host=hostName)                                
                                            samples_line = re.split("\s+", lines[2])
                                            min_latency = re.split("\s+", lines[3])
                                            max_latency = re.split("\s+", lines[4])
                                            mean_line = re.split("\s+", lines[5])

                                            port.with_property("esxi_host", hostName)                                                       

                                            port.add_metric(
                                                Metric(key="tx_total_samples", value=samples_line[1])
                                            )
                                            port.add_metric(
                                                Metric(key="rx_total_samples", value=samples_line[2])
                                            )

                                            port.add_metric(
                                                Metric(key="tx_min_latency", value=min_latency[1])
                                            )
                                            port.add_metric(
                                                Metric(key="tx_max_latency", value=max_latency[1])
                                            )
                                            port.add_metric(
                                                Metric(key="tx_mean", value=mean_line[1])
                                            )
                                            port.add_metric(
                                                Metric(key="rx_min_latency", value=min_latency[2])
                                            )
                                            port.add_metric(
                                                Metric(key="rx_max_latency", value=max_latency[2])
                                            )
                                            port.add_metric(
                                                Metric(key="rx_mean", value=mean_line[2])
                                            )
                                            port.add_parent(host)
                                            ports.append(port) 
                            except Exception as e:
                                logger.error(f"Exception occured while parsing command output {port_result}. Exception Type: {type(e).__name__}")
                                logger.exception(f"Exception Message: {e}")
                else:
                    logger.error(f'Number of commands executed does not match with the number of outputs retrieved')
            else:
                logger.error(f'Found zero DvSPortSets')
        else:
            logger.error(f'"nsxdp-cli vswitch instance list command output is empty: "{vSwitchInstanceListCmdOutput}')
    logger.info(f'Collected {len(ports)} ports from host {hostName}')            
    return ports

def add_port_relationships(vSwitchInstanceListCmdOutput: str, vlans_by_name: {}, ports: List[Port], vmsByName: {}, suiteAPIClient) -> List:
    RelAddedToVMObjects = []   
    delimiterChar = "."
    vmMacNameDict = {}
    portToVLANRelationsAdded = 0
    with Timer(logger, f'Port to vLAN and VM relationship creation'):
        try:
            if ports is not None and ports:
                port_by_name = {}
                for port in ports:
                    port_by_name.update({port.get_key().name: port})

                if vSwitchInstanceListCmdOutput is not None and vSwitchInstanceListCmdOutput:
                    portset_results = re.split("DvsPortset", vSwitchInstanceListCmdOutput)
                    for port_result in portset_results:
                        rows = re.split("\n", port_result)[3:]
                        numOfRows = len(rows)                            
                        for rowIndex in range(numOfRows):
                            columns = re.split("\s+", rows[rowIndex])                    
                            if len(columns) > 3:
                                #process columns
                                port = port_by_name.get(f'{columns[2]}')                        
                                vlan = vlans_by_name.get(columns[6])
                                if port is None or vlan is None:
                                    logger.info(f'Port ({columns[2]}:{port}) to VLAN ({columns[6]}:{vlan}) relationship was not created.')
                                else:
                                    port.add_parent(vlan)
                                    portToVLANRelationsAdded += 1
                                    logger.info(f'Port ({port.get_key().name}) to VLAN ({vlan.get_key().name}) relationship was created')
                                if port is not None: 
                                    vmNICMacaddress = columns[4].strip()
                                    clientName = columns[1].strip()
                                    subRowIndex = rowIndex
                                    while ((subRowIndex + 1) < numOfRows):                                
                                        subRowIndex += 1                                    
                                        nextRowColumns = re.split("\s+", rows[subRowIndex])                               
                                        numOfColumns = len(nextRowColumns)                                
                                        if(numOfColumns >= 2 and nextRowColumns[2].strip() == ""):
                                            clientName = clientName + nextRowColumns[1].strip()
                                        else:                                    
                                            break                             
                                                            
                                    vmName = ""
                                    lastIndex = clientName.rfind(delimiterChar)
                                    if lastIndex != -1:
                                        vmName = clientName[:lastIndex]
                                        vmMacNameDict[vmNICMacaddress] = vmName                                                  
                                        vms = vmsByName.get(vmName)
                                        if port is None or vms is None:
                                            logger.info(f'Port ({columns[2]}:{port}) to VM({vmName}:{vms}) relationship was not created.')
                                        else:                       
                                            port.with_property("vm", vmName)
                                            if len(vms) == 1:                                        
                                                port.add_parent(vms[0])
                                                logger.info(f'Port ({port.get_key().name}) to VM ({vms[0].get_key().name}) relationship was created')
                                                RelAddedToVMObjects.append(vms[0])                                        
                                            elif len(vms) > 1:
                                                vmMOID = getVMMOID(suiteAPIClient,vmName,vmNICMacaddress)
                                                for vm in vms:                                            
                                                    if vm.get_identifier_value("VMEntityObjectID") == vmMOID:                                                
                                                        port.add_parent(vm)
                                                        logger.info(f'Port ({port.get_key().name}) to VM ({vm.get_key().name}) relationship was created')
                                                        RelAddedToVMObjects.append(vm)
                                            else:
                                                logger.info(f'Port ({columns[2]}:{port}) to VM({vmName}:{vms}) relationship was not created.')
                else:
                    logger.error(f'"nsxdp-cli vswitch instance list command output is empty: "{vSwitchInstanceListCmdOutput}')
            else:
                logger.info(f'No relations to ports can be added. Port list is empty - ({ports})')
        except Exception as e:
            logger.error(f"Exception occured while creating VM relationship to ports. Exception Type: {type(e).__name__}")
            logger.exception(f"Exception Message: {e}")
    logger.info(f'Added VM relationships to {len(RelAddedToVMObjects)} Ports')
    logger.info(f'Added VLAN relationships to {portToVLANRelationsAdded} Ports')        
    return RelAddedToVMObjects, vmMacNameDict

def getVMMOID(suite_api_client: SuiteApiClient, vmName: str, vmNICMacaddress: str ) -> str:
    try:
        vmResourceIDs = []
        MAC_ADDRESS_PROPERTY_NAME = "mac_address"
        VM_MOID_PROPERTY_NAME = "summary|MOID"
        vmMOID = ""
        vmResourceIDsStr=""
        vmsResponse = suite_api_client.get(f'/api/resources?name={vmName}&adapterKind=VMWARE&resourceKind=VirtualMachine&_no_links=true')  
        vmResourceList = json.loads(vmsResponse.content)["resourceList"]
        for vmResource in vmResourceList:
            vmResourceIDs.append(vmResource["identifier"]) 
        for vmResourceID in vmResourceIDs:
            vmResourceIDsStr = vmResourceIDsStr + "resourceId=" + vmResourceID + "&"
        
        propResponse = suite_api_client.get(f'/api/resources/properties?{vmResourceIDsStr}_no_links=true')
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
        logger.error(f"An error occurred: {e}")
        return None
    
    logger.info(f'Response: {vmsResponse.content}')
    return None


