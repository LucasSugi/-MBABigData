# Databricks notebook source
# Pyspark
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DoubleType
from pyspark.sql.types import StructField, StructType

# Functools
from functools import reduce

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("year","")
dbutils.widgets.text("bucket_name","")

# Get parameters
year = dbutils.widgets.get("year")
bucket_name = dbutils.widgets.get("bucket_name")

# Filepath
filepath_prova = get_filepath(bucket_name,"generic+microdados_gov","bronze","itens-prova")

# Get file to process
files_prova = get_files(".*{}.*".format(year),filepath_prova)
files_prova = "/".join(files_prova[0].split('/')[-2:])
print("Processing file: {}".format(files_prova))

# COMMAND ----------

# Schema prova
prova_schema = StructType([StructField("CO_POSICAO",StringType(),True),StructField("SG_AREA",StringType(),True),StructField("CO_ITEM",StringType(),True),StructField("TX_GABARITO",StringType(),True),StructField("CO_HABILIDADE",StringType(),True),StructField("IN_ITEM_ABAN",StringType(),True),StructField("TX_MOTIVO_ABAN",StringType(),True),StructField("NU_PARAM_A",DoubleType(),True),StructField("NU_PARAM_B",DoubleType(),True),StructField("NU_PARAM_C",DoubleType(),True),StructField("TX_COR",StringType(),True),StructField("CO_PROVA",StringType(),True),StructField("TP_LINGUA",StringType(),True),StructField("IN_ITEM_ADAPTADO",StringType(),True)])

# Read files
df_prova = (
  read_table(bucket_name,"generic+microdados_gov","bronze",files_prova,options = {"sep":";","encoding":"latin1","header":"True"},schema=prova_schema,table_format="csv")
)
df_prova = df_prova.select("*",f.lit(year).alias("ANO_PROVA"))

# COMMAND ----------

# Column to select
base_columns = ["ANO_PROVA","CO_PROVA","CO_ITEM"]
select_columns = ["CO_POSICAO","TX_GABARITO","NU_PARAM_A","NU_PARAM_B","NU_PARAM_C"] + base_columns

# Struct columns
struct_columns = f.struct([f.col(column).alias(column) for column in select_columns if column not in base_columns]).alias("INFO_ITEM")

# Transform prova
df_prova = (
  df_prova
  .withColumn("CO_ITEM",f.dense_rank().over(Window().partitionBy("ANO_PROVA","SG_AREA","CO_PROVA").orderBy(f.col("CO_ITEM").asc())).cast("string"))
  .withColumn("RN",f.row_number().over(Window().partitionBy("CO_ITEM","CO_PROVA").orderBy(f.col("CO_POSICAO").asc())))
  .filter(f.col("RN")==1)
  .select(*select_columns)
  .select(*base_columns,struct_columns)
#   .groupBy("ANO_PROVA","CO_PROVA")
#   .agg(
#     f.map_from_entries(f.collect_list(f.struct("CO_ITEM","INFO_ITEM"))).alias("INFO_ITEM")
#   )
)

# COMMAND ----------

# Save
(
  df_prova
  .write
  .format("delta")
  .mode("overwrite")
  .option("replaceWhere","ANO_PROVA == {}".format(year))
  .partitionBy("ANO_PROVA")
  .save("s3://prd-ifood-data-lake-sandbox-generic/generic+microdados_gov/silver/itens-prova")
)
