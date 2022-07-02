# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

def schema2Table(df,table):
  data = []
  for s in df.schema:
    if(s.dataType == StringType()):
      data.append((table,s.name,"","string"))

    else:
      fields = df.schema[s.name].jsonValue()["type"]["fields"]
      for field in fields:
        data.append((table,s.name,field["name"],field["type"]))

  return data

# COMMAND ----------

df_itens_prova = spark.read.format("delta").load("")
df_enem = spark.read.format("delta").load("")
df_censo_escolar = spark.read.format("delta").load("")

# COMMAND ----------

data1 = schema2Table(df_enem,"enem")
data2 = schema2Table(df_censo_escolar,"censo_escolar")
data3 = schema2Table(df_itens_prova,"itens_prova")

# COMMAND ----------

spark.createDataFrame(data1 + data2 + data3,schema=["Tabela","Coluna 1","Coluna 2","Tipo"]).display()