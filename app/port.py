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
                object_kind="port",
                identifiers=[Identifier(key="port", value=uuid), Identifier(key="host", value=host)],
            )
        )


def get_ports(ssh: SSHClient, host: Object):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    uplinkPorts = []
    ports = []
    results = {}
    commands = ['nsxdp-cli ens latency system dump -s 0', 'nsxdp-cli ens latency system dump -s 1', 'nsxdp-cli ens latency system clear -s 0', 'nsxdp-cli ens latency system clear -s 1', 'nsxdp-cli vswitch instance list']
    results = []
    vSwitchInstanceListCmdOutput = ""

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'{host.get_key().name} Port Collection'):
        try:
            for command in commands:
                stdin, stdout, stderr = ssh.exec_command(command)
                error = stderr.read().decode()
                output = stdout.read().decode()
                results.append(output)
        except paramiko.AuthenticationException:
            logger.error(
                f'Authentication failed, please verify your credentials'
            )
        except paramiko.SSHException as sshException:
            logger.error(
                f'Could not establish SSH connection: {sshException}'
            )
        except Exception as e:
            logger.error(
                f'An error occurred: {e}'
            )
        else:
            logger.debug(f'Successfully connected and ran command({command})')
        finally:
            logger.debug(f'Results ({results})')
        if len(results) != 5:            
            logger.error(f'Error processing ssh command results')
        else:        
            try:
                vSwitchInstanceListCmdOutput = results[4]
                if vSwitchInstanceListCmdOutput is not None:
                    UPLINK_NAME = "vmnic128"
                    VDR_PORT = "vdrPort"
                    portset_results = re.split("DvsPortset", vSwitchInstanceListCmdOutput)
                    for port_result in portset_results:
                        rows = re.split("\n", port_result)[3:]
                        numOfRows = len(rows)                            
                        for rowIndex in range(numOfRows):
                            columns = re.split("\s+", rows[rowIndex])        
                            if(len(columns) > 3 and columns[5].strip().casefold() == UPLINK_NAME.casefold() and columns[3].strip().casefold() != VDR_PORT.casefold()): 
                                uplinkPortID = columns[2].strip()
                                uplinkPorts.append(uplinkPortID)
            except Exception as e:
                    logger.error(f'Error processing ssh command results: {e}') 

            for i in range(2):
                port_results = re.split("PortID:\s+", results[i])[1:]        
                for port_result in port_results:
                    try:
                        lines = re.split("\n", port_result)
                        if len(lines) > 1:
                            uuid = re.split("\s+", lines[0])[0]
                            for uplinkPortID in uplinkPorts:
                                if uplinkPortID == uuid.strip():
                                    port = Port(name="PortID: " + uplinkPortID, uuid=uplinkPortID, host=host.get_key().name)                                
                                    samples_line = re.split("\s+", lines[2])
                                    min_latency = re.split("\s+", lines[3])
                                    max_latency = re.split("\s+", lines[4])
                                    mean_line = re.split("\s+", lines[5])
                                    max_line = re.split("\s+", lines[6])                   

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
                                        Metric(key="tx_max", value=max_line[1])
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
                                    port.add_metric(
                                        Metric(key="rx_max", value=max_line[2])
                                    )

                                    port.add_parent(host)
                                    ports.append(port) 
                    except Exception as e:
                        logger.error(f'Error processing ssh command results: {e}')
    logger.debug(f'Number of ports found: ({len(ports)})')            
    return ports, vSwitchInstanceListCmdOutput

def add_port_relationships(vSwitchInstanceListCmdOutput: str, vlans_by_name: {}, ports: List[Port], vmsByName: {}, suiteAPIClient) -> List:
    RelAddedToVMObjects = []   
    delimiterChar = "."    
    with Timer(logger, f'Port to vLAN and VM relationship creation'):
        try:
            port_by_name = {}
            for port in ports:
                port_by_name.update({port.get_key().name: port})
            portset_results = re.split("DvsPortset", vSwitchInstanceListCmdOutput)
            for port_result in portset_results:
                rows = re.split("\n", port_result)[3:]
                numOfRows = len(rows)                            
                for rowIndex in range(numOfRows):
                    columns = re.split("\s+", rows[rowIndex])                    
                    if len(columns) > 3:
                        #process columns
                        port = port_by_name.get(f'PortID: {columns[2]}')                        
                        vlan = vlans_by_name.get(columns[6])
                        if port is None or vlan is None:
                            logger.info(f'No Connection Port({columns[2]}:{port}) vLAN({columns[6]}:{vlan})')
                        else:
                            logger.info(f'Connection created - Port({port.get_key().name}) vLAN({vlan.get_key().name})')                            
                            port.add_parent(vlan)
                        
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
                                vms = vmsByName.get(vmName)
                                if port is None or vms is None:
                                    logger.info(f'No Connection Port({columns[2]}:{port}) VM({vmName}:{vms})')
                                else:                       
                                    if len(vms) == 1:                                        
                                        port.add_parent(vms[0])
                                        RelAddedToVMObjects.append(vms[0])                                        
                                    elif len(vms) > 1:
                                        vmMOID = getVMMOID(suiteAPIClient,vms,vmName, vmNICMacaddress)
                                        for vm in vms:                                            
                                            if vm.get_identifier_value("VMEntityObjectID") == vmMOID:                                                
                                                port.add_parent(vm)
                                                RelAddedToVMObjects.append(vm)
                                    else:
                                        logger.info(f'No Connection Port({columns[2]}:{port}) VM({vmName}:{vms})')
        except Exception as e:
            logger.error(f'An error occurred: {e}'
            )           
    return RelAddedToVMObjects

def getVMMOID(suite_api_client: SuiteApiClient, vms: List, vmName: str, vmNICMacaddress: str ) -> str:
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


