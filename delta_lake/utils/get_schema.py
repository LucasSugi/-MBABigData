# Databricks notebook source
# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("bucket_name","")

# Get parameters
bucket_name = dbutils.widgets.get("bucket_name")

# COMMAND ----------

# Read data
df_itens_prova = read_table(bucket_name,"generic+microdados_gov","silver","itens-prova")
df_enem = read_table(bucket_name,"generic+microdados_gov","silver","enem")
df_censo_escolar = read_table(bucket_name,"generic+microdados_gov","silver","censo-escolar")

# COMMAND ----------

# Get schema for each table
schema_enem = schema2Table(df_enem,"Enem")
schema_censo_escolar = schema2Table(df_censo_escolar,"Censo Escolar")
schema_itens_prova = schema2Table(df_itens_prova,"Itens Prova")

# COMMAND ----------

# Union all schemas
schema_all = schema_enem.unionByName(schema_censo_escolar).unionByName(schema_itens_prova)

# Show
schema_all.display()