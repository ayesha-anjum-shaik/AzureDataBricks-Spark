-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### SparkSQL Introduction
-- MAGIC 1. Create database
-- MAGIC 1. Data tab in UI
-- MAGIC 1. SHOW, DESCRIBE command
-- MAGIC 1. Find current database
-- MAGIC 1. Create managed table using Python, SQL

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW databases;

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES;

-- COMMAND ----------

SHOW TABLES IN default;

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC circuits_df.write.format("parquet").saveAsTable("demo.circuits_table")

-- COMMAND ----------

USE demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED circuits_table;

-- COMMAND ----------

SELECT * FROM demo.circuits_table;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS circuits_sql
AS
SELECT * FROM demo.circuits_table;

-- COMMAND ----------

DESC EXTENDED demo.circuits_sql;

-- COMMAND ----------

