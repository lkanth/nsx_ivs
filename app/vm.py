#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
from typing import Any
from typing import List

from aria.ops.object import Object
from aria.ops.suite_api_client import SuiteApiClient
from constants import VCENTER_ADAPTER_KIND

logger = logging.getLogger(__name__)

def get_vms(suite_api_client: SuiteApiClient, adapter_instance_id: str, hostname: str) -> Any:
    vmsByName = {}
    
    try:
        logger.info(f'Making VCF Operations REST API call to retrieve a list of Virtual Machines for the ESXi host {hostname}')
        vms: List[Object] = suite_api_client.query_for_resources(
            {
                "adapterKind": [VCENTER_ADAPTER_KIND],
                "resourceKind": ["VirtualMachine"],
                "adapterInstanceId": [adapter_instance_id],
                "propertyConditions" : {
                    "conjunctionOperator" : "OR",
                    "conditions" : [ {
                        "key" : "summary|parentHost",
                        "operator" : "EQ",
                        "stringValue" : hostname
                    }]
                },
            }
        )
        logger.info(f'VCF Operations REST API call returned response with {len(vms)} Virtual Machine objects')
                   
        for vm in vms:        
            vmsByName.setdefault(vm.get_key().name, []).append(vm)
    except Exception as e:
        logger.error(f'Exception occured while getting a list of virtual machines from VCF Operations. Exception Type: {type(e).__name__}')
        logger.exception(f'Exception Message: {e}') 
    return vmsByName
   
    
    
