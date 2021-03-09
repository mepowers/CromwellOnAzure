# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 09:08:28 2021

@author: mepowers
"""

from azure.cosmos import cosmos_client
#import json
from pandas.io.json import json_normalize
import pandas as pd
import flatten_json as fj

## Authenticate with CosmosDB

# To retrieve the url and key, use the Azure CLI snippet 
# under "Authenticate with Client" at:  
# https://pypi.org/project/azure-cosmos/

url = "https://coa-704349517.documents.azure.com:443/"
key = '7BBMzohWbqvYc80FDarL4DqdRckWC8NKDRl9EheYRwnXa40nzPHaubdmJYIF9DmU7W6na99p99XHRswBeaD1hw=='

database_name = 'TES'
container_name = 'Tasks'
stored_procedure = 'coa-demo' #Can call and execute an existing Stored Procedure

client = cosmos_client.CosmosClient(url, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

## Call an existing Stored Procedure, and print the output:
#result = container.scripts.execute_stored_procedure(sproc=stored_procedure, partition_key='01') 
#print(result)

# Alternatively, execute specific queries on the Database Container:
# for item in container.query_items(
#         query='SELECT * FROM c where startswith(c.description,"d86dff9d-0233") AND c.state != "COMPLETE"',
#         enable_cross_partition_query=True):
#     print(json.dumps(item, indent=True))
output = pd.DataFrame()
for item in container.query_items(
        query='SELECT * FROM c where startswith(c.description,"d86dff9d-0233") AND c.state = "COMPLETE"',
        enable_cross_partition_query=True):
    flat = json_normalize(fj.flatten_json(item))
    output = output.append(flat, ignore_index=True)

of_interest = output[[
 "id",
 "state",
 "name",
 "description",
 "resources_cpu_cores",
 "resources_preemptible",
 "resources_ram_gb",
 "resources_disk_gb",
 "resources_zones",
 "resources_vm_info_vm_size",
 "resources_vm_info_vm_series",
 "resources_vm_info_vm_low_priority",
 "resources_vm_info_vm_price_per_hour",
 "resources_vm_info_vm_memory_in_gb",
 "resources_vm_info_vm_number_of_cores",
 "resources_vm_info_vm_resource_disk_size_in_gb",
 "resources_vm_info_vm_max_data_disk_count"]]
print(of_interest)

# Write to CSV
of_interest.to_csv (r'somesome.csv', index = False, header=True)
