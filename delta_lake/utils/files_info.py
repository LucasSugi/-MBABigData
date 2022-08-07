# Databricks notebook source
# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("year","")
dbutils.widgets.text("bucket_name","")

# Get parameters
year = dbutils.widgets.get("year")
bucket_name = dbutils.widgets.get("bucket_name")

# Filepath
filepath_censo_escolar = get_filepath(bucket_name,"generic+microdados_gov","bronze","censo-escolar")
filepath_enem = get_filepath(bucket_name,"generic+microdados_gov","bronze","enem")
filepath_itens_prova = get_filepath(bucket_name,"generic+microdados_gov","bronze","itens-prova")

# COMMAND ----------

# Get data
df_censo_escolar = spark.read.format("csv").option("sep",";").load(filepath_censo_escolar)
df_enem = spark.read.format("csv").option("sep",";").load(filepath_enem)
df_itens_prova = spark.read.format("csv").option("sep",";").load(filepath_itens_prova)

# COMMAND ----------

# Count
print("censo_escolar",df_censo_escolar.count())
print("enem",df_enem.count())
print("itens_prova",df_itens_prova.count())

# COMMAND ----------

# Sizes
print("censo_escolar",get_size_from_filepath(filepath_censo_escolar,"gb"))
print("enem",get_size_from_filepath(filepath_enem,"gb"))
print("itens_prova",get_size_from_filepath(filepath_itens_prova,"mb"))
