# Databricks notebook source
# MAGIC %md
# MAGIC ## Access Azure Data Lake Storage using the service principal
# MAGIC 1. Register Active Directory Application/ Service Principal
# MAGIC 1. Generate a password/secret for the application
# MAGIC 1. Set the spark config using service principal clientid/password
# MAGIC 1. Assign role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'databrickscourse-secretscope', key = 'databrickscourse-client-id')
tenant_id = dbutils.secrets.get(scope = 'databrickscourse-secretscope', key = 'databrickscourse-tenant-id')
client_secret = dbutils.secrets.get(scope = 'databrickscourse-secretscope', key = 'databrickscourse-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@databrickscoursedludemy.dfs.core.windows.net/",
  mount_point = "/mnt/databrickscoursedludemy/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/databrickscoursedludemy/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/databrickscoursedludemy/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.unmount('/mnt/databrickscoursedludemy/demo')