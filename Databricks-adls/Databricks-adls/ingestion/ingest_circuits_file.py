# Databricks notebook source
# MAGIC %md
# MAGIC #### Read the CSV file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.databrickscoursedludemy.dfs.core.windows.net",
    "v/Eaio897r1gkkGtUCCisZSTfimEYFjedlzar2+HQ3bwOGcCOMm2npl7HzTpyCO7b+MsHWxNUiZX+AStxqom9Q==")
dbutils.fs.ls("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

circuits_df = spark.read.option("header",True).csv("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

circuits_df.show()

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

circuits_df = spark.read.option("header",True).option("inferSchema",True).csv("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Select only the required columns

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

circuits_df = spark.read.option("header",True).schema(circuits_schema).csv("abfss://demo@databrickscoursedludemy.dfs.core.windows.net")

# COMMAND ----------

# DBTITLE 1, 
# MAGIC %md
# MAGIC #### Rename the columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") \
.withColumn("data_source",lit(v_data_source))

# COMMAND ----------

display(circuits_renamed_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Add ingestion date to dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date",current_timestamp()).withColumn("env",lit("Production"))

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write data to data lake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://processed@databrickscoursedludemy.dfs.core.windows.net/circuits")

# COMMAND ----------

df = spark.read.parquet("abfss://processed@databrickscoursedludemy.dfs.core.windows.net/circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

circuits_final_df.write.mode("overwrite").parquet("abfss://processed@databrickscoursedludemy.dfs.core.windows.net/circuits")