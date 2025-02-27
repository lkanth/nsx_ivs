#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
from typing import List
import traceback
from aria.ops.timer import Timer
import constants
from aria.ops.data import Metric
from aria.ops.object import Identifier
from aria.ops.object import Key
from aria.ops.object import Object


logger = logging.getLogger(__name__)

class lan(Object):
    def __init__(self, name: str, uuid: str, vdan: str, host: str):
        self.uuid = uuid
        self.parent = vdan
        self.host = host
        super().__init__(
            key=Key(
                name=name,
                # adapter_kind should match the key defined for the AdapterKind in line 4 of the describe.xml
                adapter_kind=constants.ADAPTER_KIND,
                # object_kind should match the key used for the ResourceKind in line 15 of the describe.xml
                object_kind="lan",
                identifiers=[Identifier(key="uuid", value=uuid), Identifier(key="vdan", value=vdan), Identifier(key="host", value=host)],
            )
        )

def get_lan(result: List[str], vdan: str, host: str) -> lan:
    try:
        locallan = lan(
            name=result[0]+":"+vdan,
            uuid=result[0],
            vdan=vdan,
            host=host
        )
        locallan.add_metric(
            Metric(key="prp_rx_pkts", value=result[1])
        )
        locallan.add_metric(
            Metric(key="non_prp_rx_pkts", value=result[2])
        )

        if len(result) > 7:
            locallan.add_metric(Metric(key="rx_bytes", value=result[3]))
            locallan.add_metric(Metric(key="dup_drops", value=result[4]))
            locallan.add_metric(Metric(key="sup_rx_pkts", value=result[5]))
            locallan.add_metric(Metric(key="out_of_order_drops", value=result[6]))
            locallan.add_metric(Metric(key="wrong_lan_drops", value=result[7]))
        else:
            locallan.add_metric(Metric(key="tx_bytes", value=result[3]))
            locallan.add_metric(Metric(key="tx_drops", value=result[4]))
            locallan.add_metric(Metric(key="sup_tx_pkts", value=result[5]))

    except Exception as e:
        logger.error(
            f'Error processing LAN results: {e} - {result}'
        )
        logger.debug(
            f'Trace: {traceback.format_exc()}'
        )

    return locallan