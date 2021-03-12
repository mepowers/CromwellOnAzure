# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 09:08:28 2021

@author: mepowers
"""

#When run on local machine, script will authenticate with CosmosDB account, execute a specific Stored Procedure in that account, and return the output

from azure.cosmos import cosmos_client

## Authenticate with CosmosDB
# To retrieve the url and key, use the Azure CLI snippet 
# under "Authenticate with Client" at:  
# https://pypi.org/project/azure-cosmos/

url = "<url>"
key = '<key>'

database_name = '<database-name>'
container_name = '<container-name>'
stored_procedure = '<name-of-stored-procedure>' #Can call and execute an existing Stored Procedure

client = cosmos_client.CosmosClient(url, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# Call an existing Stored Procedure, and print the output:
result = container.scripts.execute_stored_procedure(sproc=stored_procedure, partition_key='01') 
print(result)

