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
from paramiko import SSHClient
import traceback
import pyVim
from pyVim.connect import Disconnect
from pyVim.connect import SmartConnect

import aria.ops.adapter_logging as logging
import constants
from aria.ops.adapter_instance import AdapterInstance
from aria.ops.data import Metric
from aria.ops.data import Property
from aria.ops.definition.adapter_definition import AdapterDefinition
from aria.ops.definition.units import Units
from aria.ops.result import CollectResult
from aria.ops.result import EndpointResult
from aria.ops.result import TestResult
from aria.ops.timer import Timer
from aria.ops.object import Object

from port import get_ports
from constants import ADAPTER_KIND
from constants import ADAPTER_NAME
from constants import HOST_IDENTIFIER
from constants import PORT_IDENTIFIER
from constants import USER_CREDENTIAL 
from constants import PASSWORD_CREDENTIAL 
from constants import VCENTER_ADAPTER_KIND
#import switch
import port
import vdan
import lan
import node
import host
import vlan
import vm
from vlan import get_vlans
from vlan import get_switch_property
from port import add_port_relationships
from host import get_hosts
from host import get_host_property
from vdan import get_vdans
from node import get_nodes
from lan import get_lans
from vm import get_vms
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

        vdan = definition.define_object_type("vdan", "vDAN")
        vdan.define_string_identifier("uuid", "UUID")
        vdan.define_string_identifier("host", "ESXi Server")
        vdan.define_string_property("mac", "MAC Address")
        vdan.define_string_property("vlan_id", "vLAN")
        vdan.define_string_property("fc_port_id", "fc Port ID")
        vdan.define_metric("vdan_age", "vDAN Age")

        node = definition.define_object_type("node", "Node")
        node.define_string_identifier("uuid", "UUID")
        node.define_string_identifier("host", "ESXi Server")
        node.define_string_property("mac", "MAC Address")
        node.define_string_property("vlan_id", "vLAN")
        node.define_string_property("type", "Node Type")
        node.define_metric("node_age", "Node Age")

        lan = definition.define_object_type("lan", "LAN")
        lan.define_string_identifier("lan", "LAN")        
        lan.define_string_identifier("host", "ESXi Server")
        lan.define_string_identifier("switchID", "Switch ID")
        lan.define_string_property("name", "Name")
        lan.define_string_property("uplink1", "Uplink 1")
        lan.define_string_property("uplink2", "Uplink 2")
        lan.define_string_property("policy", "Policy")
        lan.define_string_property("status", "Status")
        lan.define_string_property("esxi", "ESXi Host")
        lan.define_numeric_property("switch", "Switch")
        
        '''
        lan.define_metric("prp_rx_pkts", "MAC Address")
        lan.define_metric("non_prp_rx_pkts", "vLAN")
        lan.define_metric("tx_bytes", "Tx Bytes")
        lan.define_metric("tx_drops", "Tx Drops")
        lan.define_metric("sup_tx_drops", "Sup Tx Drops")
        lan.define_metric("rx_bytes", "Rx Bytes")
        lan.define_metric("dup_drops", "Dup Drops")
        lan.define_metric("sup_rx_pkts", "Sup Rx Packats")
        lan.define_metric("sup_tx_pkts", "Sup Tx Packats")
        lan.define_metric("out_of_order_drops", "Out Of Order Drops")
        lan.define_metric("wrong_lan_drops", "Wrong LAN Drops")
        '''

        port = definition.define_object_type("port", "Port")
        port.define_string_identifier("port", "Port")
        port.define_string_identifier("host", "ESXi Server")
        port.define_string_property("name", "Name")
        port.define_metric("tx_total_samples", "Transmit - Total Samples")
        port.define_metric("tx_min_latency", "Transmit - Minimum Latency (us)")
        port.define_metric("tx_max_latency", "Transmit - Maximum Latency (us)")
        port.define_metric("tx_mean", "Transmit - Mean Latency (us)")
        port.define_metric("tx_max", "Transmit - Max Latency (us)")       
        port.define_metric("rx_total_samples", "Received - Total Samples")
        port.define_metric("rx_min_latency", "Received - Minimum Latency (us)")
        port.define_metric("rx_max_latency", "Received - Maximum Latency (us)")
        port.define_metric("rx_mean", "Received - Mean Latency (us)")
        port.define_metric("rx_max", "Received - Max Latency (us)")       

        return definition

def connect(adapter_instance: AdapterInstance, adapter_instance_id, client: SuiteApiClient, content) -> Any:
    """Establishes a connection with the host

    :return: ssh client an AVI session object
    :except: KeyError environment variable is not found
    """

    # Get all hosts
    with Timer(logger, "Retrieve hosts from vCenter"):       
        hosts = get_hosts(client, adapter_instance_id, content)        
    #Connect to ssh for each host and create client
    ssh_list = {}

    with Timer(logger, "create ssh sessions"):
        port = int(adapter_instance.get_identifier_value("ssh_port"))
        username = adapter_instance.get_credential_value("ssh_username")
        password = adapter_instance.get_credential_value("ssh_password")
        for host in hosts:
            try:
                controller = get_host_property(client, host, "net|mgmt_address")
                if controller is None:
                    raise ConnectionError("No host provided")
                if "ssh_username" not in adapter_instance.credentials:
                    raise ConnectionError("No username provided")
                if "ssh_password" not in adapter_instance.credentials:
                    raise ConnectionError("No password provided")

                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(hostname=controller, port=port, username=username, password=password)
                ssh_list.update({host.get_key().name: ssh})
            except paramiko.AuthenticationException:
                logger.error("Authentication failed, please verify your credentials")
            except paramiko.SSHException as sshException:
                logger.error(f"Could not establish SSH connection: {sshException}")
            except Exception as e:
                logger.error(f"An error occurred: {e}")
            else:
                logger.info(f'Connected to host({controller})')
    
    return hosts, ssh_list

def get_host(adapter_instance: AdapterInstance) -> str:
    """
    Helper method that gets the host and prepends the https protocol to the host if
    the protocol is not present
    :param adapter_instance: Adapter Instance object that holds the configuration
    :return: The host, including the protocol
    """
    host = adapter_instance.get_identifier_value("host")
    if host.startswith("http"):
        return str(host)
    else:
        return f"https://{host}"

def test(adapter_instance: AdapterInstance) -> TestResult:
    with Timer(logger, "Test connection"):       
        result = TestResult()        
        try:            
            logger.debug(f"Returning test result: {result.get_json()}")
            service_instance = _get_service_instance(adapter_instance)
            content = service_instance.RetrieveContent()
            # logger.info(f"content: {content}")
        except Exception as e:
            logger.error("Unexpected connection test error")
            logger.exception(e)
            result.with_error("Unexpected connection test error: " + repr(e))
        finally:
            return result


def collect(adapter_instance: AdapterInstance) -> CollectResult:
    with Timer(logger, "Collection"):
        result = CollectResult()
        
        with adapter_instance.suite_api_client as client:
            service_instance = _get_service_instance(adapter_instance)
            content = service_instance.RetrieveContent()
            adapter_instance_id = _get_vcenter_adapter_instance_id(client, adapter_instance)
            hosts, ssh_list = connect(adapter_instance, adapter_instance_id, client, content)            
            vlans = get_vlans(client)
            RelAddedToVMObjects = []
            with Timer(logger, "Collect Objects"):
                for host in hosts:
                    try:
                        ssh = ssh_list.get(host.get_key().name)
                        if ssh is None:
                            logger.info(f'Unable to collect from {host.get_key().name}')
                        else:
                            vdans = get_vdans(ssh, host)
                            for vdan in vdans:
                                #logger.info(f'vdan vlan property({vdan.get_property("vlan_id")[0].value})')
                                vlan = vlans.get(vdan.get_property('vlan_id')[0].value)
                                if vlan:
                                    vdan.add_parent(vlan)
                            nodes = get_nodes(ssh, host)
                            for node in nodes:
                                vlan = vlans.get(node.get_property('vlan_id')[0].value)
                                if vlan:
                                    node.add_parent(vlan)
                            lans = get_lans(ssh, host)
                            logger.info(f'LM-A LANS: {lans}')
                            ports, vSwitchInstanceListCmdOutput = get_ports(ssh, host)
                            vmsByName = get_vms(client, adapter_instance_id, content, host.get_key().name)                                                    
                            vmObjectList = add_port_relationships(vSwitchInstanceListCmdOutput, vlans, ports, vmsByName, client)
                            if len(vmObjectList) > 0:
                                for vmObject in vmObjectList:
                                    RelAddedToVMObjects.append(vmObject)                                            
                            
                            result.add_objects(vdans)
                            result.add_objects(nodes)
                            result.add_objects(lans)
                            result.add_objects(ports)
                            ssh.close()
                    except Exception as e:
                        logger.error(f'Unexpected collection error: {e}')
                try:
                    result.add_objects(hosts)                    
                    for vm in RelAddedToVMObjects:                                        
                        result.add_object(vm)
                    for vlan in vlans.values():
                        result.add_object(vlan)
                except Exception as e:
                    logger.error(f'Unexpected collection error: {e}')                

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


def _get_service_instance(
    adapter_instance: AdapterInstance,
) -> Any:  # vim.ServiceInstance
    host = adapter_instance.get_identifier_value(constants.HOST_IDENTIFIER)
    port = int(adapter_instance.get_identifier_value(constants.PORT_IDENTIFIER, 443))
    user = adapter_instance.get_credential_value(constants.USER_CREDENTIAL)
    password = adapter_instance.get_credential_value(constants.PASSWORD_CREDENTIAL)
    
    service_instance = SmartConnect(host=host, port=port, user=user, pwd=password, disableSslCertValidation=True)

    # doing this means you don't need to remember to disconnect your script/objects
    atexit.register(Disconnect, service_instance)

    return service_instance


# Get the ID of the vCenter Adapter Instance that matches the vCenter Server of this
# Extension. We use this to filter resources from VMware Aria Operations to the specific
# vCenter Server we are collecting against to prevent collisions when two objects from
# different vCenters share the same entity ID.
def _get_vcenter_adapter_instance_id(
    client: SuiteApiClient, adapter_instance: Object
) -> Optional[str]:
    ais: List[Object] = client.query_for_resources(
        {
            "adapterKind": [VCENTER_ADAPTER_KIND],
            "resourceKind": ["VMwareAdapter Instance"],
        }
    )
    vcenter_server = adapter_instance.get_identifier_value(HOST_IDENTIFIER)
    for ai in ais:
        logger.debug(
            f"Considering vCenter Adapter Instance with VCURL: {ai.get_identifier_value('VCURL')}"
        )
        if ai.get_identifier_value("VCURL") == vcenter_server:
            return _get_adapter_instance_id(client, ai)
    return None


def _get_adapter_instance_id(
    client: SuiteApiClient, adapter_instance: Object
) -> Optional[Any]:
    response = client.get(
        f"api/adapters?adapterKindKey={adapter_instance.get_key().adapter_kind}"
    )
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
