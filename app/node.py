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
                identifiers=[Identifier(key="uuid", value=uuid), Identifier(key="host", value=host)],
            )
        )

def get_nodes(ssh: SSHClient, host: Object):
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    nodes = []
    llans = []
    results = {}
    command = "nsxdp-cli ens prp stats node list"

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'{host.get_key().name} Node Collection'):
        try:
            stdin, stdout, stderr = ssh.exec_command(command)
            error = stderr.read().decode()
            result = stdout.read().decode()            
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
        finally:
            logger.debug(
                f'Successfully connected and ran command({command})'
            )

        try:
            node_results = re.split("\n", result)
            i = 2

            while i < len(node_results):
                columns = re.split("\s+", node_results[i])
                if columns[0].isnumeric():
                    uuid = columns[0]
                    mac = columns[1]
                    lnode = Node(
                        name="Node: " + uuid,
                        uuid=uuid,
                        host=host.get_key().name
                    )

                    lnode.with_property("mac", mac)
                    lnode.with_property("vlan_id", columns[2])
    
                    lnode.with_property("type", columns[3])
                    lnode.add_metric(
                        Metric(key="node_age", value=columns[4])
                    )
                    lnode.add_parent(host)
                    nodes.append(lnode)
                else:
                    i += 1
        except Exception as e:
            logger.error(
                f'Error processing ssh command results: {e} - {traceback.format_exc()}'
            )

    return nodes

