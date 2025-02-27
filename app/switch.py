#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
from typing import List

import constants
from aria.ops.data import Metric
from aria.ops.object import Identifier
from aria.ops.object import Key
from aria.ops.object import Object
import paramiko
from paramiko import SSHClient
import port
from port import Port

logger = logging.getLogger(__name__)


class Switch(Object):

    def __init__(self, name: str, uuid: str, url: str):
        """Initializes a Tenant object that represent the ResourceKind defined in line 15 of the describe.xml file.

        :param name: The  unique name of used to display the tenant
        :param uuid: A Universal Unique Identifier for the Tenant
        :param url: A URL with the AVI controller, and the tenant's UUID
        """
        self.uuid = uuid
        self.url = url
        super().__init__(
            key=Key(
                name=name,
                # adapter_kind should match the key defined for the AdapterKind in line 4 of the describe.xml
                adapter_kind=constants.ADAPTER_KIND,
                # object_kind should match the key used for the ResourceKind in line 15 of the describe.xml
                object_kind="switch",
                identifiers=[Identifier(key="uuid", value=uuid)],
            )
        )


def get_switchs(ssh: SSHClient) -> List[Switch]:
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    switches = []
    results = {}
    command = ""

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    try:
        stdin, stdout, stderr = ssh.exec_command(command)
        error = stderr.read().decode()
        results = stdout.read().decode()
    except paramiko.AuthenticationException:
        logger.debug(f"Error during switch retrieval: {error}")

    #Process results into switches


    return switches


def add_switchs_children(switches: List[Switch], ports: List[Port]) -> List[Switch]:

    for switch in switches:
        # A tenant can have many clouds related to itself
        children = filter(lambda c: c.parent_tenant_url == tenant.url, clouds)

        # Add each Cloud to their related Tenant
        for child in children:
            tenant.add_child(child)

    return tenants
