# Databricks notebook source
import requests
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
host_name = ctx.tags().get("browserHostName").get()
host_token = ctx.apiToken().get()

# COMMAND ----------

response = requests.get(
f'https://{host_name}/api/2.0/secrets/scopes/list',
headers={'Authorization': f'Bearer {host_token}'}
).json()
