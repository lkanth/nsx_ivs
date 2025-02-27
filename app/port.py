#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import re
from typing import List
import traceback
from aria.ops.timer import Timer
import constants
from aria.ops.data import Metric
from aria.ops.object import Identifier
from aria.ops.object import Key
from aria.ops.object import Object
import paramiko
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


def get_ports(ssh: SSHClient, host: Object) -> List[Port]:
    """Fetches all tenant objects from the API; instantiates a Tenant object per JSON tenant object, and returns a list
    of all tenants

    :param api: AVISession object
    :return: A list of all Switch Objects collected, along with their properties, and metrics
    """
    ports = []
    results = {}
    command = "nsxdp-cli ens latency system dump"

    # Logging key errors can help diagnose issues with the adapter, and prevent unexpected behavior.
    with Timer(logger, f'{host.get_key().name} Port Collection'):
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
        else:
            logger.debug(f'Successfully connected and ran command({command})')
        finally:
            logger.debug(f'Results ({result})')


        
        port_results = re.split("PortID:\s+", result)[1:]
        for port_result in port_results:
            try:
                lines = re.split("\n", port_result)
                if len(lines) > 1:
                    uuid = re.split("\s+", lines[0])[0]
                    port = Port(
                        name="PortID: " + uuid,
                        uuid=uuid,
                        host=host.get_key().name
                    )

                    samples_line = re.split("\s+", lines[2])
                    min_latency = re.split("\s+", lines[3])
                    max_latency = re.split("\s+", lines[4])
                    mean_line = re.split("\s+", lines[5])
                    max_line = re.split("\s+", lines[6])
                    thirtytwo = re.split("\s+", lines[7])
                    sixtyfour = re.split("\s+", lines[8])
                    nintysix = re.split("\s+", lines[9])
                    onetwentyeight = re.split("\s+", lines[10])
                    onesixty = re.split("\s+", lines[11])
                    onenintytwo = re.split("\s+", lines[12])
                    twofiftysix = re.split("\s+", lines[13])
                    fivetwelve = re.split("\s+", lines[14])
                    tentwentyfour = re.split("\s+", lines[15])
                    twentyfourtyeight = re.split("\s+", lines[16])
                    fourtyninetysix = re.split("\s+", lines[17])

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
                        Metric(key="tx_32us", value=thirtytwo[1])
                    )
                    port.add_metric(
                        Metric(key="tx_64us", value=sixtyfour[1])
                    )
                    port.add_metric(
                        Metric(key="tx_96us", value=nintysix[1])
                    )
                    port.add_metric(
                        Metric(key="tx_128us", value=onetwentyeight[1])
                    )
                    port.add_metric(
                        Metric(key="tx_160us", value=onesixty[1])
                    )
                    port.add_metric(
                        Metric(key="tx_192us", value=onenintytwo[1])
                    )
                    port.add_metric(
                        Metric(key="tx_256us", value=twofiftysix[1])
                    )
                    port.add_metric(
                        Metric(key="tx_512us", value=fivetwelve[1])
                    )
                    port.add_metric(
                        Metric(key="tx_1024us", value=tentwentyfour[1])
                    )
                    port.add_metric(
                        Metric(key="tx_2048us", value=twentyfourtyeight[1])
                    )
                    port.add_metric(
                        Metric(key="tx_4096us", value=fourtyninetysix[1])
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
                    port.add_metric(
                        Metric(key="rx_32us", value=thirtytwo[2])
                    )
                    port.add_metric(
                        Metric(key="rx_64us", value=sixtyfour[2])
                    )
                    port.add_metric(
                        Metric(key="rx_96us", value=nintysix[2])
                    )
                    port.add_metric(
                        Metric(key="rx_128us", value=onetwentyeight[2])
                    )
                    port.add_metric(
                        Metric(key="rx_160us", value=onesixty[2])
                    )
                    port.add_metric(
                        Metric(key="rx_192us", value=onenintytwo[2])
                    )
                    port.add_metric(
                        Metric(key="rx_256us", value=twofiftysix[2])
                    )
                    port.add_metric(
                        Metric(key="rx_512us", value=fivetwelve[2])
                    )
                    port.add_metric(
                        Metric(key="rx_1024us", value=tentwentyfour[2])
                    )
                    port.add_metric(
                        Metric(key="rx_2048us", value=twentyfourtyeight[2])
                    )
                    port.add_metric(
                        Metric(key="rx_4096us", value=fourtyninetysix[2])
                    )
                    port.add_parent(host)
                    ports.append(port)  
            except Exception as e:
                logger.error(f'Error processing ssh command results: {e}')
            
    logger.debug(f'Number of ports found: ({len(ports)})')            
    return ports

