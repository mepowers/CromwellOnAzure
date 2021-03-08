# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 09:08:28 2021

@author: mepowers
"""

from azure.cosmos import cosmos_client

# To retrieve the url and key, use the Azure CLI snippet 
# under "Authenticate with Client" at:  
# https://pypi.org/project/azure-cosmos/

url = "https://{coa-AccountName}.documents.azure.com:{port}/"
key = '{Connection-String}'

database_name = 'TES'
container_name = 'Tasks'
stored_procedure = 'coa-demo'

client = cosmos_client.CosmosClient(url, key)
database = client.get_database_client(database_name)
container = database.get_container_client(container_name)

result = container.scripts.execute_stored_procedure(sproc=stored_procedure, partition_key='01') 

print(result)
