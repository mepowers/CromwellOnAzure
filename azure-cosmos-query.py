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

url = "<URL>"
key = '<KEY>'
WF_ID = "<Workflow-ID>"

# ***** UPDATE FOR EACH RUN *****
write_to_csv = 1 #1=yes; 0=no
fn_prefix = "WGS-run-mixed-VMs" #for filename - brief description of run
fn_suffix = "1"
local_path = "\\local\\path\\for\\output\\"
fn = local_path + fn_prefix + "_" + WF_ID + fn_suffix + ".csv"

#Database and Container names - should be consistent across runs
database_name = '<database-name>'
container_name = '<container-name>'

#Create the Cosmos client; Connect to database and container of interest
client = cosmos_client.CosmosClient(url, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# Execute query to pull in all JSON items and all fields into single DataFrame:
output = pd.DataFrame()
for item in container.query_items(
        query='SELECT * FROM c where startswith(c.id, "' + WF_ID + '") AND c.state = "COMPLETE"',
        enable_cross_partition_query=True):
    flat = json_normalize(fj.flatten_json(item))
    output = output.append(flat, ignore_index=True)
    # print(output)

# Create new DataFrame with only fields of interest
of_interest = output[[
  "id",
  "state",
  "name",
  "description",
  "resources_cpu_cores",
  "resources_preemptible",
  "resources_ram_gb",
  "resources_disk_gb",
    # Headers for WGS runs:
 'logs_0_logs_0_stdout',
 'logs_0_logs_0_stderr',
 'logs_0_logs_0_exit_code',
 'logs_0_metadata_vm_size',
 'logs_0_metadata_vm_series',
 'logs_0_metadata_vm_low_priority',
 'logs_0_metadata_vm_price_per_hour_usd',
 'logs_0_metadata_vm_memory_in_gb',
 'logs_0_metadata_vm_number_of_cores',
 'logs_0_metadata_vm_resource_disk_size_in_gb',
 'logs_0_metadata_vm_max_data_disk_count',
 'logs_0_metadata_cromwell_rc',
 'logs_0_metadata_blobxfer_pull_duration_sec', #Blob transfer of image used to download files
 'logs_0_metadata_executor_pull_duration_sec', #Pulling docker image to VM
 'logs_0_metadata_download_duration_sec', #Downloading datasets to VM
 'logs_0_metadata_executor_duration_sec', #Actual runtime of the task
 'logs_0_metadata_upload_duration_sec',
 'logs_0_metadata_file_download_size_gb',
 'logs_0_metadata_file_upload_size_gb',
 'logs_0_metadata_disk_used_gb',
 'logs_0_metadata_disk_used_pct',
 'logs_0_start_time', #When job submitted to Batch
 'logs_0_end_time', #When discovered that Batch is done
 'logs_0_logs_0_start_time', #Likely most accurate billed start time
 'logs_0_logs_0_end_time' #Likely most accurate billed end time
  ]]
print(of_interest)

# Write to CSV on local machine
if write_to_csv == 1 :
    of_interest.to_csv (fn, index = False, header=True)
