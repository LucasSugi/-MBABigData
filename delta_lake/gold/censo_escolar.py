# Databricks notebook source
# Pyspark
import pyspark.sql.functions as f

# Functools
from functools import reduce

def count_value(column,value):
  
  # Create a counter
  _count_value = f.when(f.col(column) == value,f.lit(1)).otherwise(f.lit(0))
  
  return _count_value

def fillna(column):
  
  # Create a fillna
  _fillna = f.when(f.col(column).isNull(),f.lit(0)).otherwise(f.col(column))
  
  return _fillna

def sum_columns(columns,alias):
  
  # Sum all columns
  _sum_columns = reduce(lambda x,y: x + y,columns).alias(alias)
  
  return _sum_columns

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("year","")
dbutils.widgets.text("bucket_name","")

# Get parameters
year = dbutils.widgets.get("year")
bucket_name = dbutils.widgets.get("bucket_name")

# COMMAND ----------

# Read files
df_censo_escolar = read_table(bucket_name,"generic+microdados_gov","silver","censo-escolar")

# COMMAND ----------

# Select turno
select_turno = [count_value("TURNO.{}".format(column),"Sim").alias("QT_{}".format(column.replace("IN_",""))) for column in df_censo_escolar.select("TURNO.*").columns]

# Select alimentacao
select_alimentacao = count_value("ALIMENTACAO.IN_ALIMENTACAO","Oferece").alias("QT_ALIMENTACAO")

# Select internet
select_internet = [count_value("INTERNET.{}".format(column),"Sim") for column in df_censo_escolar.select("INTERNET.*").columns if column not in ["IN_REDES_SOCIAIS","IN_INTERNET"]]
select_internet = sum_columns(select_internet,"QT_INTERNET")

# Select equipamentos
select_equipamentos = [fillna("EQUIPAMENTOS.{}".format(column)) for column in df_censo_escolar.select("EQUIPAMENTOS.*").columns if "QT" in column]
select_equipamentos = sum_columns(select_equipamentos,"QT_EQUIPAMENTOS")

# Select infraestrutura
select_infraestrutura = [count_value("INFRAESTRUTURA.{}".format(column),"Sim") for column in df_censo_escolar.select("INFRAESTRUTURA.*").columns if "IN" in column]
select_infraestrutura = sum_columns(select_infraestrutura,"QT_INFRAESTRUTURA")

# Select base
select_base = ["ANO_CENSO","ENDERECO_ESCOLA.SG_UF","ENDERECO_ESCOLA.NO_MUNICIPIO"]

# COMMAND ----------

# Apply all selects
df_censo_escolar = df_censo_escolar.select(*select_base,*select_turno,select_alimentacao,select_internet,select_equipamentos,select_infraestrutura)

# COMMAND ----------

# Seeed for id creation
seed_id = "wU0bRB"

# Set id cidade ano
df_censo_escolar = set_id(df_censo_escolar,["ANO_CENSO","NO_MUNICIPIO","SG_UF"],seed_id,"ID_CIDADE_ANO")

# COMMAND ----------

# Select to sum columns in groupBy
select_agg_sum = [f.sum(f.col(column)).alias(column) for column in df_censo_escolar.columns if "QT" in column]

# Apply select
df_censo_escolar = (
  df_censo_escolar
  .groupBy("ID_CIDADE_ANO")
  .agg(*select_agg_sum)
)

# COMMAND ----------

# Save
write_table(
  df_censo_escolar,
  bucket_name,
  "generic+microdados_gov",
  "gold",
  "censo-escolar-dimension",
  options = {"overwriteSchema":"true"}
)
