# Databricks notebook source
# PySpark ML
from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.tuning import CrossValidator

# Time
from time import time

# COMMAND ----------

# MAGIC %run ../lib/delta_lake

# COMMAND ----------

# Set parameters
dbutils.widgets.text("bucket_name","")

# Get parameters
bucket_name = dbutils.widgets.get("bucket_name")

# COMMAND ----------

def train_rf(data):
  
  # Start time
  start = time()
  
  # Create assembles
  assembler = VectorAssembler(inputCols=data.columns[:-1], outputCol="features")
  output = assembler.transform(data)

  # Create Random Forest
  rf = RandomForestRegressor(labelCol="label", featuresCol="features")

  # Create pipeline
  pipeline = Pipeline(stages=[assembler, rf])

  # Param grid to use in test
  paramGrid = (
    ParamGridBuilder()
    .addGrid(rf.numTrees, [10])
    .addGrid(rf.maxDepth, [5])
    .build()
  )

  # Cross Validator with two folds
  crossval = CrossValidator(estimator=pipeline,estimatorParamMaps=paramGrid,evaluator=RegressionEvaluator(),numFolds=2)

  # Fit data
  cvModel = crossval.fit(data)

  # Prediciton
  predictions = cvModel.transform(data)
  
  # End time
  end = time()

  # Elapsed time (minutes)
  elapsed = round((end-start) / 60,1)

  print(f"Took: {elapsed} min")

# COMMAND ----------

# Read enem data
df_enem = read_table(bucket_name,"generic+microdados_gov","silver","enem")

# Get correct columns to test
df_enem = df_enem.select("NOTA_PROVA.*","ACERTOS.*",f.col("REDACAO.NU_NOTA_REDACAO").alias("LABEL"))
df_enem = df_enem.toDF(*[column.lower() for column in df_enem.columns]).fillna(0)

# COMMAND ----------

for i in range(5):

  # Training
  train_rf(df_enem)
