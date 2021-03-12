# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 09:08:28 2021

@author: mepowers
"""

from azure.cosmos import cosmos_client

## Authenticate with CosmosDB
# To retrieve the url and key, use the Azure CLI snippet 
# under "Authenticate with Client" at:  
# https://pypi.org/project/azure-cosmos/

# For /small/ubams test workflow in useast:
# url = "https://coa-704349517.documents.azure.com:443/"
# key = '7BBMzohWbqvYc80FDarL4DqdRckWC8NKDRl9EheYRwnXa40nzPHaubdmJYIF9DmU7W6na99p99XHRswBeaD1hw=='

url = "https://coa-2ff32443a.documents.azure.com:443"
key = 'H7Qgd4DYNABjS8J6ruHna2PEyyDPp5ZyrQmigByLn4nWjkFWanGK0jaXVrFOaW8GcrdnJZc0O2oHeU2mdxh92g=='

database_name = 'TES'
container_name = 'Tasks'
stored_procedure = 'coa-demo' #Can call and execute an existing Stored Procedure

client = cosmos_client.CosmosClient(url, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

# Call an existing Stored Procedure, and print the output:
result = container.scripts.execute_stored_procedure(sproc=stored_procedure, partition_key='01') 
print(result)

