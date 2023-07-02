# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner join

# COMMAND ----------

circuits_constructors_inner_df = circuits_df.join(constructors_df,circuits_df.circuit_id == constructors_df.constructor_id,"inner")

# COMMAND ----------

display(circuits_constructors_inner_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Left outer join

# COMMAND ----------

circuits_constructors_left_df = circuits_df.join(constructors_df,circuits_df.circuit_id == constructors_df.constructor_id,"left")

# COMMAND ----------

display(circuits_constructors_left_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Right outer join

# COMMAND ----------

circuits_constructors_right_df = circuits_df.join(constructors_df,circuits_df.circuit_id == constructors_df.constructor_id,"right")

# COMMAND ----------

display(circuits_constructors_right_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full outer join

# COMMAND ----------

circuits_constructors_full_df = circuits_df.join(constructors_df,circuits_df.circuit_id == constructors_df.constructor_id,"full")

# COMMAND ----------

display(circuits_constructors_full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi Join

# COMMAND ----------

circuits_constructors_semi_df = circuits_df.join(constructors_df,circuits_df.circuit_id == constructors_df.constructor_id,"semi")

# COMMAND ----------

display(circuits_constructors_semi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti join

# COMMAND ----------

circuits_constructors_anti_df = circuits_df.join(constructors_df,circuits_df.circuit_id == constructors_df.constructor_id,"anti")

# COMMAND ----------

display(circuits_constructors_semi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross join

# COMMAND ----------

circuits_constructors_cross_df = circuits_df.crossJoin(constructors_df)

# COMMAND ----------

display(circuits_constructors_cross_df)

# COMMAND ----------

circuits_constructors_cross_df.count()

# COMMAND ----------

int(circuits_df.count())*int(constructors_df.count())

# COMMAND ----------

