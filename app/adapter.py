#  Copyright 2022 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import atexit
import json
import sys
from typing import Any
from typing import List
from typing import Optional
from aria.ops.suite_api_client import key_to_object
from aria.ops.suite_api_client import SuiteApiClient

import paramiko
from pyVim.connect import Disconnect
from pyVim.connect import SmartConnect

import aria.ops.adapter_logging as logging
import constants
from aria.ops.adapter_instance import AdapterInstance
from aria.ops.definition.adapter_definition import AdapterDefinition
from aria.ops.definition.units import Units
from aria.ops.result import CollectResult
from aria.ops.result import EndpointResult
from aria.ops.result import TestResult
from aria.ops.timer import Timer
from aria.ops.object import Object


from constants import ADAPTER_KIND
from constants import ADAPTER_NAME
from constants import HOST_IDENTIFIER
from constants import VCENTER_ADAPTER_KIND

from vlan import get_vlans

from port import get_ports
from port import add_port_relationships

from host import get_hosts
from host import get_host_property

from vdan import get_vdans
from vdan import add_vdan_vlan_relationship
from vdan import add_vdan_vm_relationship

from node import get_nodes
from node import add_node_vlan_relationship

from lan import get_lans

from vm import get_vms

from switch import parse_ensswitch_list
from switch import get_switches
from switch import get_host_switches

logger = logging.getLogger(__name__)


def get_adapter_definition() -> AdapterDefinition:
    """
    The adapter definition defines the object types and attribute types (metric/property) that are present
    in a collection. Setting these object types and attribute types helps VMware Aria Operations to
    validate, process, and display the data correctly.
    :return: AdapterDefinition
    """
    with Timer(logger, "Get Adapter Definition"):
        definition = AdapterDefinition(ADAPTER_KIND, ADAPTER_NAME)

        definition.define_string_parameter(constants.HOST_IDENTIFIER, "vCenter Server", description="FQDN or IP address of the vCenter Server Instance.")
        definition.define_int_parameter(constants.PORT_IDENTIFIER, "vCenter Port", default=443)
        definition.define_int_parameter("ssh_port", "SSH Port", default=22)

        credential = definition.define_credential_type("isv_credentials", "NSX IsV Credential")
        credential.define_string_parameter(constants.USER_CREDENTIAL, "VCenter Username")
        credential.define_password_parameter(constants.PASSWORD_CREDENTIAL, "VCenter Password")
        credential.define_string_parameter("ssh_username", "SSH Username")
        credential.define_password_parameter("ssh_password", "SSH Password")

        # The key 'container_memory_limit' is a special key that is read by the VMware Aria Operations collector to
        # determine how much memory to allocate to the docker container running this adapter. It does not
        # need to be read inside the adapter code.
        definition.define_int_parameter(
            "container_memory_limit",
            label="Adapter Memory Limit (MB)",
            description="Sets the maximum amount of memory VMware Aria Operations can "
            "allocate to the container running this adapter instance.",
            required=True,
            advanced=True,
            default=1024,
        )
        
        vdan = definition.define_object_type("vdan", "NSX IvS vDAN")
        vdan.define_string_identifier("uuid", "UUID")
        vdan.define_string_identifier("host", "ESXi Server")
        vdan.define_string_property("mac", "MAC Address")
        vdan.define_string_property("vlan_id", "vLAN")
        vdan.define_numeric_property("fc_port_id", "fc Port ID")
        vdan.define_string_property("esxi_host", "ESXI Host")
        vdan.define_string_property("vm", "Virtual Machine")
        vdan.define_numeric_property("vdan_id", "VDAN Identifier")
        vdan.define_numeric_property("switch_id", "Switch ID")
        vdan.define_string_property("switch_uuid", "Switch UUID")
        vdan.define_string_property("switch_name", "Switch Name")
        vdan.define_metric("vdan_age", "VDAN Age")
        vdan.define_metric("lanA_prpTxPkts", "LAN-A PRP Transmitted Packets")
        vdan.define_metric("lanA_nonPRPPkts", "LAN-A non PRP Transmitted Packets")
        vdan.define_metric("lanA_txBytes", "LAN-A Transmitted Bytes")
        vdan.define_metric("lanA_txDrops", "LAN-A Transmitted Drops")
        vdan.define_metric("lanA_supTxPkts", "LAN-A Transmitted Packets Suppressed")
        vdan.define_metric("lanB_prpTxPkts", "LAN-B PRP Transmitted Packets")
        vdan.define_metric("lanB_nonPRPPkts", "LAN-B non PRP Transmitted Packets")
        vdan.define_metric("lanB_txBytes", "LAN-B Transmitted Bytes")
        vdan.define_metric("lanB_txDrops", "LAN-B Transmitted Drops")
        vdan.define_metric("lanB_supTxPkts", "LAN-B Transmitted Packets Suppressed")

        node = definition.define_object_type("node", "NSX IvS Node")
        node.define_string_identifier("uuid", "UUID")
        node.define_string_identifier("host", "ESXi Server")
        node.define_string_property("esxi_host", "ESXI Host")
        node.define_string_property("mac", "MAC Address")
        node.define_string_property("vlan_id", "vLAN")
        node.define_string_property("type", "Node Type")
        node.define_metric("node_age", "Node Age")

        lan = definition.define_object_type("lan", "NSX IvS LAN")
        lan.define_string_identifier("uuid", "UUID")
        lan.define_string_property("uplink1", "Uplink 1")
        lan.define_string_property("uplink2", "Uplink 2")
        lan.define_string_property("policy", "Policy")
        lan.define_string_property("status", "Status")
        lan.define_string_property("switch_uuid", "Switch UUID")
        lan.define_string_property("switch_name", "Switch Name")

        port = definition.define_object_type("port", "NSX IvS Port")
        port.define_string_identifier("uuid", "UUID")
        port.define_string_identifier("host", "ESXi Server")
        port.define_string_property("vlan_id", "vLAN")
        port.define_string_property("esxi_host", "ESXI Host")
        port.define_string_property("vm", "Virtual Machine")
        port.define_numeric_property("switch_id", "Switch ID")
        port.define_string_property("switch_uuid", "Switch UUID")
        port.define_string_property("switch_name", "Switch Name")
        port.define_metric("tx_total_samples", "Transmit - Total Samples")
        port.define_metric("tx_min_latency", "Transmit - Minimum Latency", Units.TIME.MICROSECONDS)
        port.define_metric("tx_max_latency", "Transmit - Maximum Latency", Units.TIME.MICROSECONDS)
        port.define_metric("tx_mean", "Transmit - Mean Latency", Units.TIME.MICROSECONDS)
        port.define_metric("tx_max", "Transmit - Max Latency", Units.TIME.MICROSECONDS)       
        port.define_metric("rx_total_samples", "Received - Total Samples")
        port.define_metric("rx_min_latency", "Received - Minimum Latency", Units.TIME.MICROSECONDS)
        port.define_metric("rx_max_latency", "Received - Maximum Latency", Units.TIME.MICROSECONDS)
        port.define_metric("rx_mean", "Received - Mean Latency", Units.TIME.MICROSECONDS)
        port.define_metric("rx_max", "Received - Max Latency", Units.TIME.MICROSECONDS)       

        return definition

def test(adapter_instance: AdapterInstance) -> TestResult:
    with Timer(logger, "Test connection"): 
        logger.info(f'Setup adapter for testing connection')      
        result = TestResult()
        logger.info(f'Setup adapter for testing connection - Successful')        
        try:
            host = adapter_instance.get_identifier_value(constants.HOST_IDENTIFIER)
            port = int(adapter_instance.get_identifier_value(constants.PORT_IDENTIFIER, 443))
            user = adapter_instance.get_credential_value(constants.USER_CREDENTIAL)
            password = adapter_instance.get_credential_value(constants.PASSWORD_CREDENTIAL)

            if not host:
                result.with_error("vCenter server name not found. Please supply vCenter server name")
            if type(port) is not int or port == '' or port < 1 or port > 65535:
                result.with_error("vCenter connection Port must be an integer from 1-65535")
            if not user:
                result.with_error("vCenter user name not found. Please supply vCenter user name")
            if not password:
                result.with_error("vCenter password not found. Please supply vCenter user password")
            
            service_instance = SmartConnect(host=host, port=port, user=user, pwd=password, disableSslCertValidation=True)
            if not service_instance:
                result.with_error("vCenter connection test failed.")

             # doing this means you don't need to remember to disconnect your script/objects
            atexit.register(Disconnect, service_instance)         
            
        except Exception as e:
            logger.error(f"Exception occured while testing connection. Exception Type: {type(e).__name__}")
            logger.exception(f"Exception Message: {e}")            
            result.with_error("Unexpected connection test error: " + repr(e))
        finally:
            logger.info(f"Returning test result: {result.get_json()}")
            return result


def collect(adapter_instance: AdapterInstance) -> CollectResult:
    with Timer(logger, "Collect data"):
        logger.info(f'Setup adapter for collecting data - Version: 07252025-08:00:00')
        result = CollectResult()
        logger.info(f'Setup adapter for collecting data - Successful')
        try:
            SSHPort = int(adapter_instance.get_identifier_value("ssh_port"))
            SSHUserName = adapter_instance.get_credential_value("ssh_username")
            SSHpassword = adapter_instance.get_credential_value("ssh_password")
            
            if SSHPort:
                logger.info(f'Retrieved SSH port for the ESXi hosts')
            else:
                logger.error(f'SSH port is NULL or Empty or Zero. Supply SSH port number in IvS adapter configuration in VCF Operations')
                result.with_error("SSH port is NULL or Empty. Supply SSH port number in IvS adapter configuration in VCF Operations")
                return result        
            
            if SSHUserName:
                logger.info(f'Retrieved SSH username for the ESXi hosts')
            else:
                logger.error(f'SSH username is NULL or Empty. Supply SSH username and password for ESXi hosts in IvS adapter configuration in VCF Operations')
                result.with_error("SSH username is NULL or Empty. Supply SSH username and password for ESXi hosts in IvS adapter configuration in VCF Operations")
                return result 
                
            if SSHpassword:
                logger.info(f'Retrieved SSH user password for the ESXi hosts')
            else:
                logger.error(f'SSH password is NULL or Empty. Supply SSH username and password for ESXi hosts in IvS adapter configuration in VCF Operations')
                result.with_error("SSH password is NULL or Empty. Supply SSH username and password for ESXi hosts in IvS adapter configuration in VCF Operations")
                return result
            
            if adapter_instance:
                logger.info(f'Got valid adapter instance')
            else:
                logger.error(f'Adapter instance is NULL or Empty. Check your VCF Operations Credentials')
                result.with_error("Adapter instance is NULL or Empty. Check your VCF Operations Credentials")
                return result
            
            with adapter_instance.suite_api_client as client:
                
                if client:
                    logger.info(f'Got valid VCF Operations Suite API Client')
                else:
                    logger.error(f'VCF Operations Suite API Client is NULL or empty. Check your VCF Operations Credentials')
                    result.with_error("VCF Operations Suite API Client is NULL or empty. Check your VCF Operations Credentials")
                    return result
                
                service_instance = _get_service_instance(adapter_instance)
                if service_instance:
                    logger.info(f'Successfully connected to vCenter and retrieved vim.serviceinstance.')
                else:
                    logger.error(f'vim.serviceinstance is NULL or empty. Cannot connect to vCenter. Check your vCenter credentials.')
                    result.with_error("vim.serviceinstance is NULL or empty. Cannot connect to vCenter. Check your vCenter credentials.")
                    return result
                
                content = service_instance.RetrieveContent()
                if content:
                    logger.info(f'Successfully retrieved vim.serviceinstance content.')
                else:
                    logger.error(f'vim.serviceinstance content is NULL or empty.')
                    result.with_error("vim.serviceinstance content is NULL or empty.")
                    return result

                
                adapter_instance_id = _get_vcenter_adapter_instance_id(client, adapter_instance)
                if adapter_instance_id:
                    logger.info(f'Successfully retrieved vCenter adapter instance ID {adapter_instance_id} from VCF Operations.')
                else:
                    logger.error(f'vCenter Adapter instance ID is None. Check if vCenter adapter is configured in VCF Operations')
                    result.with_error("vCenter Adapter instance ID is None. Check if vCenter adapter is configured in VCF Operations")
                    return result
                
                logger.info(f'Get a list of ESXi hosts from VCF Operations')
                hosts = get_hosts(client, adapter_instance_id, content)
                noOfHosts = len(hosts)
            
                if hosts and noOfHosts > 0:
                    logger.info(f'Retrieved {noOfHosts} hosts from VCF Operations')
                else:
                    logger.error(f'Collection cannot proceeed as there are no ESXi hosts')
                    result.with_error("Collection cannot proceeed as there are no ESXi hosts")
                    return result  
                
                logger.info(f'Get a list of distributed switches from VCF Operations')
                distSwitchesDict = get_switches(client,adapter_instance_id)
                logger.info(f'Get a list of VLANs from VCF Operations')
                vlansDict, portGroupSwitchDict = get_vlans(client,adapter_instance_id, content, distSwitchesDict)
                RelAddedToVMObjects = []
                masterLANList = []
                masterHostToSwitchDict = {}
                hostCount = 0
        
                for host in hosts:
                    try:
                        hostCount += 1
                        hostName = host.get_key().name
                        logger.info(f'*********** Starting data collection from host {hostName} - Host count: {hostCount} ***********')
                        hostAddress = get_host_property(client, host, "net|mgmt_address")
                        if hostAddress:
                            sshClient = paramiko.SSHClient()
                            sshClient.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                            sshClient.connect(hostname=hostAddress, port=SSHPort, username=SSHUserName, password=SSHpassword, timeout=constants.SSH_CONNECT_TIMEOUT)
                            transport = sshClient.get_transport()
                            transport.set_keepalive(60) 
                        else:
                            logger.error(f'Host management address property value not found for host object: {hostName}')
                            logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                            continue
                    except paramiko.AuthenticationException as e:
                        logger.error(f'Authentication failed, please verify your credentials. Exception Type: {type(e).__name__}')
                        logger.exception(f'Exception Message: {e}')
                        logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                        continue  
                    except paramiko.SSHException as e:
                        logger.error(f'Could not establish SSH connection. Exception Type: {type(e).__name__}')
                        logger.exception(f'Exception Message: {e}')
                        logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                        continue
                    except Exception as e:
                        logger.error(f'An error occurred, Exception Type: {type(e).__name__}')
                        logger.exception(f'Exception Message: {e}')
                        logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                        continue
                    else:
                        logger.info(f'Connected to host({hostAddress})')

                    try:
                        if not sshClient:
                            logger.error(f'SSH connection to {hostName} has failed. Unable to collect data from host {hostName}')
                            logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                            continue
                        else:
                            commands = ['nsxdp-cli ens switch list','nsxdp-cli vswitch instance list']
                            cmdOutput = []
                            caughtCMDException = False                        
                            for command in commands:
                                try:
                                    logger.info(f'Running command "{command}" on host {hostName}')
                                    stdin, stdout, stderr = sshClient.exec_command(command, timeout=constants.SSH_COMMAND_TIMEOUT)
                                    error = stderr.read().decode().strip()
                                    output = stdout.read().decode()
                                    cmdOutput.append(output)
                                    exit_status = stdout.channel.recv_exit_status()
                                    if exit_status == 0:
                                        logger.info(f'Successfully ran the command "{command}" on host {hostName}')
                                    else:
                                        logger.error(f'Command failed with exit status {exit_status}. Error: {error}')
                                except paramiko.AuthenticationException as e:
                                    logger.error(f'Authentication failed, please verify your credentials. Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
                                    logger.exception(f'Exception Message: {e}')
                                    caughtCMDException = True
                                    break
                                except paramiko.SSHException as e:
                                    logger.error(f'SSH error occurred. Exception Type: {type(e).__name__}')
                                    logger.exception(f"Exception Message: {e}")
                                    caughtCMDException = True
                                    break
                                except Exception as e:
                                    logger.error(f'Exception occured while executing command "{command}". Exception Type: {type(e).__name__}')
                                    logger.exception(f'Exception Message: {e}')
                                    caughtCMDException = True
                                    break
                                else:
                                    logger.info(f'Command "{command}" output is ({output})')
                            
                            if caughtCMDException:
                                logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                                continue

                            ensSwitchListCmdOutput = cmdOutput[0]
                            vSwitchInstanceListCmdOutput = cmdOutput[1]

                            if ensSwitchListCmdOutput:
                                parsedENSSwitchList = parse_ensswitch_list(ensSwitchListCmdOutput)
                                if parsedENSSwitchList and len(parsedENSSwitchList) > 0:
                                    ensSwitchIDList = []
                                    for ensSwitch in parsedENSSwitchList:
                                        if "swID" in ensSwitch:
                                            ensSwitchIDList.append(ensSwitch['swID'])
                                    if not ensSwitchIDList or len(ensSwitchIDList) == 0:
                                        logger.info(f'No ENS switches are configured on host {hostName}')
                                        logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                                        continue
                                else:
                                    logger.info(f'No ENS switches are configured on host {hostName}')
                                    logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                                    continue
                            else:
                                logger.info(f'No ENS switches are configured on host {hostName}')
                                logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                                continue
                            
                            if not vSwitchInstanceListCmdOutput:
                                logger.info(f'vSwitch instance List command output on host {hostName} is null or empty.')
                                logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                                continue
                            
                            hostToSwitchDict = get_host_switches(host, vSwitchInstanceListCmdOutput, parsedENSSwitchList)
                            if not masterHostToSwitchDict or len(masterHostToSwitchDict) == 0:
                                masterHostToSwitchDict = hostToSwitchDict
                            else:
                                for key, value in hostToSwitchDict.items():
                                    masterHostToSwitchDict[key] = value
                            
                            
                            vmsByName = get_vms(client, adapter_instance_id, hostName)
                            ports = get_ports(sshClient, host, vSwitchInstanceListCmdOutput, ensSwitchIDList, masterHostToSwitchDict)
                            result.add_objects(ports)
                            logger.info(f'Added {len(ports)} collected port objects from host {hostName} for VCF Operations consumption')
                            vmObjectList, vmMacNameDict = add_port_relationships(vSwitchInstanceListCmdOutput, vlansDict, ports, vmsByName, client)

                            vdans = get_vdans(sshClient, host, vSwitchInstanceListCmdOutput, ensSwitchIDList, masterHostToSwitchDict)
                            result.add_objects(vdans)
                            logger.info(f'Added {len(vdans)} collected VDAN objects from host {hostName} for VCF Operations consumption')
                            vDANVMList = add_vdan_vm_relationship(vdans, vmMacNameDict, vmsByName, client)
                            add_vdan_vlan_relationship(hostName, vdans, vlansDict, portGroupSwitchDict)
                                                      

                            lans = get_lans(sshClient, host, vSwitchInstanceListCmdOutput, ensSwitchIDList, distSwitchesDict, masterLANList, masterHostToSwitchDict)
                            for lanObj in lans:
                                masterLANList.append(lanObj)
                            
                            nodes = get_nodes(sshClient, host)
                            result.add_objects(nodes)
                            logger.info(f'Added {len(nodes)} collected node objects from host {hostName} for VCF Operations consumption')
                            add_node_vlan_relationship(hostName, nodes, vlansDict, portGroupSwitchDict, masterHostToSwitchDict)

                            if len(vmObjectList) > 0:
                                for vmObject in vmObjectList:
                                    RelAddedToVMObjects.append(vmObject)
                            if len(vDANVMList) > 0:
                                for vDANVMObject in vDANVMList:
                                    if vDANVMObject not in RelAddedToVMObjects:
                                        RelAddedToVMObjects.append(vmObject)                                         
                            sshClient.close()
                            logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                    except Exception as e:
                        logger.error(f"Exception occured while collecting data from host {hostName}. Exception Type: {type(e).__name__}")
                        logger.exception(f'Exception Message: {e}')
                        logger.info(f'*********** Data collection from host {hostName} is complete ***********\n')
                    finally:
                        if sshClient:
                            sshClient.close()
                try:
                    for switch in distSwitchesDict.values():
                        result.add_object(switch)
                    logger.info(f'Added {len(distSwitchesDict)} related distributed switch objects for VCF Operations consumption')

                    result.add_objects(masterLANList)
                    logger.info(f'Added {len(masterLANList)} collected LAN objects for VCF Operations consumption')

                    result.add_objects(hosts)
                    logger.info(f'Added {len(hosts)} related Host objects for VCF Operations consumption')                                  
                    for vm in RelAddedToVMObjects:                                        
                        result.add_object(vm)
                    logger.info(f'Added {len(RelAddedToVMObjects)} related VM objects for VCF Operations consumption') 
                    distPortGroupObjectsAdded = 0
                    for distPortObjectList in vlansDict.values():
                        for distPortObjectEntry in distPortObjectList:
                            if "DistPortGroupObject" in distPortObjectEntry:
                                distPortObject = distPortObjectEntry['DistPortGroupObject']
                                result.add_object(distPortObject)
                                distPortGroupObjectsAdded += 1
                    logger.info(f'Added {distPortGroupObjectsAdded} related Distributed Port objects for VCF Operations consumption')
                except Exception as e:
                    logger.error(f"Exception occured while adding objects for VCF Operations consumption. Exception Type: {type(e).__name__}")
                    logger.error(f'Unexpected collection error: {e}')
                    result.with_error("Unexpected collection error "+ repr(e))
        except Exception as e:
            logger.error(f'An error occurred, Exception Type: {type(e).__name__}')
            logger.exception(f'Exception Message: {e}')
            result.with_error("Unexpected collection error "+ repr(e))         

    logger.debug(f"Returning collection result {result.get_json()}")
    return result

def get_endpoints(adapter_instance: AdapterInstance) -> EndpointResult:
    with Timer(logger, "Get Endpoints"):
        result = EndpointResult()
        # In the case that an SSL Certificate is needed to communicate to the target,
        # add each URL that the adapter uses here. Often this will be derived from a
        # 'host' parameter in the adapter instance. In this Adapter we don't use any
        # HTTPS connections, so we won't add any. If we did, we might do something like
        # this:
        # result.with_endpoint(adapter_instance.get_identifier_value("host"))
        #
        # Multiple endpoints can be returned, like this:
        # result.with_endpoint(adapter_instance.get_identifier_value("primary_host"))
        # result.with_endpoint(adapter_instance.get_identifier_value("secondary_host"))
        #
        # This 'get_endpoints' method will be run before the 'test' method,
        # and VMware Aria Operations will use the results to extract a certificate from
        # each URL. If the certificate is not trusted by the VMware Aria Operations
        # Trust Store, the user will be prompted to either accept or reject the
        # certificate. If it is accepted, the certificate will be added to the
        # AdapterInstance object that is passed to the 'test' and 'collect' methods.
        # Any certificate that is encountered in those methods should then be validated
        # against the certificate(s) in the AdapterInstance.
        logger.debug(f"Returning endpoints: {result.get_json()}")
        return result


def _get_service_instance(adapter_instance: AdapterInstance) -> Any:  # vim.ServiceInstance
    try:
        service_instance = None

        host = adapter_instance.get_identifier_value(constants.HOST_IDENTIFIER)
        port = int(adapter_instance.get_identifier_value(constants.PORT_IDENTIFIER, 443))
        user = adapter_instance.get_credential_value(constants.USER_CREDENTIAL)
        password = adapter_instance.get_credential_value(constants.PASSWORD_CREDENTIAL)

        if not host:
            logger.error("Get service instance failed: vCenter server name not found. Please supply vCenter server name")
            return service_instance
        if type(port) is not int or port == '' or port < 1 or port > 65535:
            logger.error("Get service instance failed: vCenter connection Port must be an integer from 1-65535")
            return service_instance
        if not user:
            logger.error("Get service instance failed: vCenter user name not found. Please supply vCenter user name")
            return service_instance
        if not password:
            logger.error("Get service instance failed: vCenter password not found. Please supply vCenter user password")
            return service_instance
        
        service_instance = SmartConnect(host=host, port=port, user=user, pwd=password, disableSslCertValidation=True)

        # doing this means you don't need to remember to disconnect your script/objects
        atexit.register(Disconnect, service_instance)
    except Exception as e:
        logger.error(f"Exception occured while establishing connection with vCenter. Exception Type: {type(e).__name__}")
        logger.exception(f"Exception Message: {e}")
    return service_instance


# Get the ID of the vCenter Adapter Instance that matches the vCenter Server of this
# Extension. We use this to filter resources from VMware Aria Operations to the specific
# vCenter Server we are collecting against to prevent collisions when two objects from
# different vCenters share the same entity ID.
def _get_vcenter_adapter_instance_id(client: SuiteApiClient, adapter_instance: Object) -> Optional[str]:
    try:
        vcenter_server = adapter_instance.get_identifier_value(HOST_IDENTIFIER)
        if not vcenter_server:
            logger.error("Get vCenter Adapter instance failed: vCenter server name not found. Please supply vCenter server name")
            return None
        
        ais: List[Object] = client.query_for_resources(
            {
                "adapterKind": [VCENTER_ADAPTER_KIND],
                "resourceKind": ["VMwareAdapter Instance"],
            }
        )
        
        for ai in ais:
            logger.debug(f"Considering vCenter Adapter Instance with VCURL: {ai.get_identifier_value('VCURL')}")
            if ai.get_identifier_value("VCURL") == vcenter_server:
                return _get_adapter_instance_id(client, ai)
    except Exception as e:
        logger.error(f"Exception occured while getting vCenter Adapter instance ID from VCF Operations. Exception Type: {type(e).__name__}")
        logger.exception(f"Exception Message: {e}")
    return None


def _get_adapter_instance_id(client: SuiteApiClient, adapter_instance: Object) -> Optional[Any]:
    response = client.get(f"api/adapters?adapterKindKey={adapter_instance.get_key().adapter_kind}")
    if response.status_code < 300:
        for ai in json.loads(response.content).get("adapterInstancesInfoDto", []):
            adapter_instance_key = key_to_object(ai.get("resourceKey")).get_key()
            if adapter_instance_key == adapter_instance.get_key():
                return ai.get("id")
    return None

# Main entry point of the adapter. You should not need to modify anything below this line.
def main(argv: List[str]) -> None:
    logging.setup_logging("adapter.log")
    # Start a new log file by calling 'rotate'. By default, the last five calls will be
    # retained. If the logs are not manually rotated, the 'setup_logging' call should be
    # invoked with the 'max_size' parameter set to a reasonable value, e.g.,
    # 10_489_760 (10MB).
    logging.rotate()
    logger.info(f"Running adapter code with arguments: {argv}")
    if len(argv) != 3:
        # `inputfile` and `outputfile` are always automatically appended to the
        # argument list by the server
        logger.error("Arguments must be <method> <inputfile> <ouputfile>")
        sys.exit(1)

    method = argv[0]
    try:
        if method == "test":
            test(AdapterInstance.from_input()).send_results()
        elif method == "endpoint_urls":
            get_endpoints(AdapterInstance.from_input()).send_results()
        elif method == "collect":
            collect(AdapterInstance.from_input()).send_results()
        elif method == "adapter_definition":
            result = get_adapter_definition()
            if type(result) is AdapterDefinition:
                result.send_results()
            else:
                logger.info(
                    "get_adapter_definition method did not return an AdapterDefinition"
                )
                sys.exit(1)
        else:
            logger.error(f"Command {method} not found")
            sys.exit(1)
    finally:
        logger.info(Timer.graph())
        sys.exit(0)


if __name__ == "__main__":
    main(sys.argv[1:])
