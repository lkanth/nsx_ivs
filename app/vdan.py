#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import re
import traceback
from typing import List
from aria.ops.timer import Timer
import constants
from aria.ops.data import Metric
from aria.ops.object import Identifier
from aria.ops.object import Key
from aria.ops.object import Object
import paramiko
from paramiko import SSHClient
import port
from port import getVMMOID

logger = logging.getLogger(__name__)

class vDAN(Object):

    def __init__(self, name: str, uuid: str, host:str):
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
                object_kind="vdan",
                identifiers=[Identifier(key="uuid", value=uuid), Identifier(key="host", value=host)],
            )
        )

def get_vdans(ssh: SSHClient, host: Object, vSwitchInstanceListCmdOutput: str):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    vdanObjects = []
    commands = []
    results = []
    hostName = host.get_key().name

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'vDAN objects collection on {host.get_key().name} '):
        if vSwitchInstanceListCmdOutput is not None and vSwitchInstanceListCmdOutput:
            prpDvsPortsetNumbers = re.findall(r'^DvsPortset-(\d+)\s*\(.*', vSwitchInstanceListCmdOutput, re.MULTILINE)
            for prpDvsPortNumber in prpDvsPortsetNumbers:
                commands.append("nsxdp-cli ens prp stats vdan list -s " + str(prpDvsPortNumber))
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

            if len(results) == len(commands):
                for result in results:
                    try:
                        vdanResults = parse_vdan_output(result)
                        for vdan in vdanResults:
                            if "vdanIndex" in vdan:
                                uuid = str(vdan["vdanIndex"]) + "_" + hostName                    
                                vdanObj = vDAN(
                                    name=str(vdan["vdanIndex"]),
                                    uuid=uuid,
                                    host=hostName
                                )
                                vdanObj.with_property("esxi_host",hostName)
                                if "vdanIndex" in vdan:
                                    vdanObj.with_property("vdan_id",vdan["vdanIndex"])
                                if "mac" in vdan:
                                    vdanObj.with_property("mac", vdan["mac"])
                                if "vlanID" in vdan:
                                    vdanObj.with_property("vlan_id", vdan["vlanID"])
                                if "fcPortID" in vdan:
                                    vdanObj.with_property("fc_port_id", vdan["fcPortID"])
                                if "vDANAge" in vdan:
                                    vdanObj.with_metric("vdan_age", vdan["vDANAge"])

                                if "lanA" in vdan and "prpTxPkts" in vdan["lanA"]:
                                    vdanObj.with_metric("lanA_prpTxPkts", vdan["lanA"]["prpTxPkts"])   
                                if "lanA" in vdan and "nonPRPPkts" in vdan["lanA"]:
                                    vdanObj.with_metric("lanA_nonPRPPkts", vdan["lanA"]["nonPRPPkts"])
                                if "lanA" in vdan and "txBytes" in vdan["lanA"]:
                                    vdanObj.with_metric("lanA_txBytes", vdan["lanA"]["txBytes"]) 
                                if "lanA" in vdan and "txDrops" in vdan["lanA"]:
                                    vdanObj.with_metric("lanA_txDrops", vdan["lanA"]["txDrops"]) 
                                if "lanA" in vdan and "supTxPkts" in vdan["lanA"]:
                                    vdanObj.with_metric("lanA_supTxPkts", vdan["lanA"]["supTxPkts"])

                                if "lanB" in vdan and "prpTxPkts" in vdan["lanB"]:
                                    vdanObj.with_metric("lanB_prpTxPkts", vdan["lanB"]["prpTxPkts"])   
                                if "lanB" in vdan and "nonPRPPkts" in vdan["lanB"]:
                                    vdanObj.with_metric("lanB_nonPRPPkts", vdan["lanB"]["nonPRPPkts"])
                                if "lanB" in vdan and "txBytes" in vdan["lanB"]:
                                    vdanObj.with_metric("lanB_txBytes", vdan["lanB"]["txBytes"]) 
                                if "lanB" in vdan and "txDrops" in vdan["lanB"]:
                                    vdanObj.with_metric("lanB_txDrops", vdan["lanB"]["txDrops"]) 
                                if "lanB" in vdan and "supTxPkts" in vdan["lanB"]:
                                    vdanObj.with_metric("lanB_supTxPkts", vdan["lanB"]["supTxPkts"])                     
                                
                                vdanObj.add_parent(host)
                                vdanObjects.append(vdanObj)
                            else:
                                logger.error(f'VDAN Index does not exist in parsed VDAN list command output: {vdanResults}') 
                    except:
                        logger.error(f"Exception occured while parsing command output {result}. Exception Type: {type(e).__name__}")
                        logger.exception(f"Exception Message: {e}")
            else:
                logger.error(f'Number of commands executed does not match with the number of outputs retrieved')               
        else:
            logger.info(f'Found zero DvSPortSets')
    logger.info(f'Collected {len(vdanObjects)} VDAN objects from host {hostName}')
    return vdanObjects

def parse_vdan_output(text):
    lines = text.strip().splitlines()
    vdans = []

    # Regex for vDAN line with lanA
    main_line_pattern = re.compile(
        r"^(\d+)\s+([\da-f:]+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(lanA)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)"
    )
    
    # Regex for lanB line
    lanb_line_pattern = re.compile(
        r"^\s*(lanB)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)"
    )

    current_common = {}

    for line in lines:
        line = line.strip()
        if not line or line.startswith('===') or line.startswith('Total PRP'):
            continue

        match_main = main_line_pattern.match(line)
        match_lanb = lanb_line_pattern.match(line)

        if match_main:
            (vdan_index, mac, vlan_id, fcport_id, age, lan, prp_tx, nonprp, txbytes, txdrops, suptx) = match_main.groups()
            # Save common data
            current_common = {
                'vdanIndex': int(vdan_index),
                'mac': mac,
                'vlanID': int(vlan_id),
                'fcPortID': int(fcport_id),
                'vDANAge': int(age)
            }
            vdans.append({
                **current_common,
                'lanA':
                {'prpTxPkts': int(prp_tx),
                'nonPRPPkts': int(nonprp),
                'txBytes': int(txbytes),
                'txDrops': int(txdrops),
                'supTxPkts': int(suptx)}
            })

        elif match_lanb:
            (lan, prp_tx, nonprp, txbytes, txdrops, suptx) = match_lanb.groups()
            for key in vdans:
            	if (key["vdanIndex"] == current_common["vdanIndex"]):
                  key["lanB"] = {
                    'prpTxPkts': int(prp_tx),
                    'nonPRPPkts': int(nonprp),
                    'txBytes': int(txbytes),
                    'txDrops': int(txdrops),
                    'supTxPkts': int(suptx)
                  }
    return vdans

def add_vdan_vm_relationship(vdans: List, vmMacNameDict:dict, vmsByName: dict, suiteAPIClient):
    RelAddedToVMObjects = []
    with Timer(logger, f'VDAN to VM relationship creation'):
        for vdanObj in vdans:
            try:
                vmMacAddress = vdanObj.get_property('mac')[0].value
                if vmMacAddress in vmMacNameDict:
                    vmName = vmMacNameDict[vmMacAddress]
                    vms = vmsByName.get(vmName)
                    if vms is None or not vms:
                        logger.info(f'VM {vmName} does not exist for relationship creation)')
                    else:
                        vdanObj.with_property("vm", vmName)                       
                        if len(vms) == 1:                                        
                            vdanObj.add_parent(vms[0])
                            logger.info(f'VDAN ({vdanObj.get_key().name}) to VM ({vms[0].get_key().name}) relationship was created')
                            RelAddedToVMObjects.append(vms[0])                                        
                        elif len(vms) > 1:
                            vmMOID = getVMMOID(suiteAPIClient,vmName,vmMacAddress)
                            for vm in vms:                                            
                                if vm.get_identifier_value("VMEntityObjectID") == vmMOID:                                                
                                    vdanObj.add_parent(vm)
                                    logger.info(f'VDAN ({vdanObj.get_key().name}) to VM ({vm.get_key().name}) relationship was created')
                                    RelAddedToVMObjects.append(vm)
                        else:
                            logger.info(f'No relation exists between vdan and VM {vmName}')
            except Exception as e:
                logger.error(f'Exception occured while creating VM "{vmName}" relationship to VDAN "{vdanObj.get_key().name}". Exception Type: {type(e).__name__}')
                logger.exception(f"Exception Message: {e}")
    logger.info(f'Added VM relationships to {len(RelAddedToVMObjects)} Vdans')
    return RelAddedToVMObjects



    