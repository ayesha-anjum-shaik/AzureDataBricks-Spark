# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount Azure Data Lake Storage using the service principal
# MAGIC 1. Get client_id, tenant_id, client_secret from key vault
# MAGIC 1. Set the spark config using service principal clientid/password
# MAGIC 1. Call file system utility mount to mount the storage
# MAGIC 1. List all mounts and unmount

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'databrickscrse-keyvault', key = 'databrickscourse-client-id')
tenant_id = dbutils.secrets.get(scope = 'databrickscrse-keyvault', key = 'databrickscourse-tenant-id')
client_secret = dbutils.secrets.get(scope = 'databrickscrse-keyvault', key = 'databrickscourse-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.databrickscoursedludemy.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.databrickscoursedludemy.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.databrickscoursedludemy.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.databrickscoursedludemy.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.databrickscoursedludemy.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@databrickscoursedludemy.dfs.core.windows.net"))