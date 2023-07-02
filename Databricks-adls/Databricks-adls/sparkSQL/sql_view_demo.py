# Databricks notebook source
# MAGIC %md
# MAGIC ### Access data frames using SQL
# MAGIC 1. Create temporary views on dataframes
# MAGIC 1. Access the view from sql cell
# MAGIC 1. Access the view from python cell
# MAGIC 1. Create global views on dataframes
# MAGIC 1. Access the view from sql cell
# MAGIC 1. Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------


circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------


display(circuits_df)

# COMMAND ----------


circuits_df.createTempView("v_circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM v_circuits
# MAGIC WHERE location = "Kuala Lumpur"

# COMMAND ----------


loc = "Melbourne"

# COMMAND ----------


loc_df = spark.sql(f"SELECT * FROM v_circuits where location = 'Melbourne'")

# COMMAND ----------


construcors_df = spark.read.parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------


construcors_df.createOrReplaceGlobalTempView("gv_global_constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES in global_temp;