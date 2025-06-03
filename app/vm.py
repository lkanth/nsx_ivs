#  Copyright 2023 VMware, Inc.
#  SPDX-License-Identifier: Apache-2.0
import logging
import json
from typing import Any
from typing import List

from aria.ops.object import Object
from aria.ops.result import CollectResult
from aria.ops.suite_api_client import SuiteApiClient
from constants import VCENTER_ADAPTER_KIND
from pyVmomi import vim

logger = logging.getLogger(__name__)

def get_vms(suite_api_client: SuiteApiClient, adapter_instance_id: str, content: Any, hostname: str) -> Any:
    container = content.rootFolder  # starting point to look into
    view_type = [vim.VirtualMachine]  # object types to look for
    recursive = True  # whether we should look into it recursively
    container_view = content.viewManager.CreateContainerView(container, view_type, recursive)
    result = []
    
    
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

    vmsByName = {}           
    for vm in vms:        
        vmsByName.setdefault(vm.get_key().name, []).append(vm) 

    return vmsByName
   
    
    
