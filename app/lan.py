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
                identifiers=[Identifier(key="lan", value=uuid), Identifier(key="host", value=host), Identifier(key="switchID", value=switchID)],
            )
        )


def get_lans(ssh: SSHClient, host: Object):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    hostname = host.get_key().name
    lanObjectList = []
    commands = ['nsxcli -c get ens prp config 0', 'nsxcli -c get ens prp config 1']
    results = []

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'{host.get_key().name} LAN Collection'):
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
            logger.debug(f'Successfully connected and ran commands({commands})')
        finally:
            logger.debug(f'Results ({results})')
        try:
            if len(results) != 2:            
                logger.error(f'Error processing ssh command results')
            else:
                for i in range(len(results)):                   
                    parsed_lan_output = parse_lan_output(results[i])
                    lanList = []
                    for key in parsed_lan_output:
                        if key.startswith("Lan"):
                            lanList.append(key)
                    if len(lanList) > 0:
                        switchID = parsed_lan_output["prp_config"]["switch"]
                        switchIDStr = str(switchID)
                        for lanItem in lanList:
                            if "status" in parsed_lan_output[lanItem]:                                
                                uuid = lanItem + "_" + hostname + "_switch_" + switchIDStr
                                lan = Lan(name=lanItem, uuid=uuid, host=hostname, switchID=switchIDStr)                               
                                lan.with_property("esxi", hostname)
                                lan.with_property("switch", switchID)
                                lan.with_property("status", parsed_lan_output[lanItem]["status"])
                                if "uplink1" in parsed_lan_output[lanItem]:
                                    lan.with_property("uplink1", value=parsed_lan_output[lanItem]["uplink1"])
                                if "uplink2" in parsed_lan_output[lanItem]:
                                    lan.with_property("uplink2", value=parsed_lan_output[lanItem]["uplink2"])
                                if "policy" in parsed_lan_output[lanItem]:
                                    lan.with_property("policy", value=parsed_lan_output[lanItem]["policy"])
                                lan.add_parent(host)
                                lanObjectList.append(lan)
        except Exception as e:
            logger.error(f'Error processing ssh command results: {e}')
    logger.debug(f'Number of Lans found: ({len(lanObjectList)})') 
    return lanObjectList


def parse_lan_output(output):
    result = {}

    # Extract date/time
    date_line = re.search(r'^[A-Z][a-z]{2} [A-Z][a-z]{2} \d{1,2} \d{4} UTC \d{2}:\d{2}:\d{2}\.\d{3}', output, re.MULTILINE)
    if date_line:
        result['timestamp'] = date_line.group()

    # Extract PRP switch config
    switch_match = re.search(r'PRP Config for switch\s+(\d+)', output)
    if switch_match:
        result['prp_config'] = {'switch': int(switch_match.group(1))}

    # Extract PRP uplink
    uplink_match = re.search(r'PRP uplink\(channel\)\s+(\S+)', output)
    if uplink_match:
        result['prp_config']['uplink_channel'] = uplink_match.group(1)

    # Extract LanA and LanB info
    lan_names = re.findall(r'^\s*(Lan\w+)', output, re.MULTILINE)
    distinct_lan_names = sorted(set(lan_names))

    for lan in distinct_lan_names:
        lan_info = {}
        for field in ['uplink1', 'uplink2', 'policy', 'status']:
            match = re.search(rf'{lan} {field}\s+(\S+)', output)
            if match:
                lan_info[field] = match.group(1)
        result[lan] = lan_info

    # Extract Red Box MAC
    mac_match = re.search(r'Red Box MAC\s+([0-9a-f:]+)', output, re.IGNORECASE)
    if mac_match:
        result['red_box_mac'] = mac_match.group(1)

    # Extract Supervision Multicast
    sm_match = re.search(r'Default Supervision Multicast\s+([0-9a-f:]+)', output, re.IGNORECASE)
    if sm_match:
        result['default_supervision_multicast'] = sm_match.group(1)

    return result