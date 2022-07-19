# Databricks notebook source
# Set parameters
dbutils.widgets.text("bucket_name","")

# Get parameters
bucket_name = dbutils.widgets.get("bucket_name")

# To store all sizes
sizes = []

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Tables on silver
layer = "silver"
tables = ["censo-escolar","enem","itens-prova"]

# Get sizes from silver
for table in tables:
  size = get_size_from_delta("{}/generic+microdados_gov/{}/{}".format(bucket_name,layer,table))
  size = size / (1024**3)
  sizes.append({"table":table,"size":size})

# COMMAND ----------

# Tables on gold
layer = "gold"
tables = ["enem-fact","enem-dimension-participante","enem-dimension-questionario-socio-economico","censo-escolar-dimension"]

# Get sizes from gold
for table in tables:
  size = get_size_from_delta("{}/generic+microdados_gov/{}/{}".format(bucket_name,layer,table))
  size = size / (1024**3)
  sizes.append({"table":table,"size":size})

# COMMAND ----------

# Create df
sizes_df = spark.createDataFrame(sizes)

# COMMAND ----------

# Display
sizes_df.sort(f.col("size").desc()).display()
