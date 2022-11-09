# Databricks notebook source
import requests
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host_name = ctx.tags().get("browserHostName").get()
host_token = ctx.apiToken().get()

# COMMAND ----------

notebook_folder = '/Users/hubert.dudek@databrickster.com'

response = requests.get(
f'https://{host_name}/api/2.0/workspace/list',
headers={'Authorization': f'Bearer {host_token}'},
json={'path': notebook_folder}
).json()

# COMMAND ----------

for notebook in response['objects']:
    if notebook['object_type'] == 'NOTEBOOK':
        dbutils.notebook.run(notebook['path'], 1800)

# COMMAND ----------


