# Databricks notebook source
# requests
from requests import get

# zip
from zipfile import ZipFile

# os
from os import path

# Regex
from re import search, sub

# Numpy
from numpy import identity

# Pyspark
import pyspark.sql.functions as f
from pyspark.sql.types import *

# Unicodedata
from unicodedata import normalize as unicodedata_normalize, combining as unicodedata_combining

# COMMAND ----------

def aws_mount(bucket_name,mount_name):
  
  # Mounting
  dbutils.fs.mount("s3a://{}".format(bucket_name), "/mnt/{}".format(mount_name))
  
def get_filepath(bucket,namespace,layer,table):
  
  # Check if layer its correct
  if (layer not in ["transient","bronze","silver","gold"]):
    raise Exception("layer : '{}' its invalid. The allowed values are 'transient', 'bronze', 'silver' and 'gold'".format(layer))
  
  # Build filepath for correct layer
  _filepath = "{}/{}/{}/{}".format(bucket,namespace,layer,table)
  
  return _filepath
  
def read_table(bucket,namespace,layer,table,options=None,schema=None,table_format="delta"):
  
  # Get filepath
  _filepath = get_filepath(bucket,namespace,layer,table)
  
  # Create reader
  reader_df = spark.read.format(table_format)
  
  # Set schema if was passed
  if(schema):
    reader_df = reader_df.schema(schema)
    
  # Set options if was passed
  if(options):
    for option in options:
      reader_df = reader_df.option(option,options[option])
      
  # Load
  df = reader_df.load(_filepath)
  
  return df

def write_table(df,bucket,namespace,layer,table,options=None,partitionBy=None,mode="overwrite",table_format="delta"):
  
  # Get filepath
  _filepath = get_filepath(bucket,namespace,layer,table)
  
  # Create writer
  writer_df  = df.write.format(table_format).mode(mode)
  
  # Set option if was passed
  if(options):
    for option in options:
      writer_df = writer_df.option(option,options[option])
    
  # Set partition by if was passed
  if(partitionBy):
    writer_df = writer_df.partitionBy(partitionBy)
    
  # Write
  print("Writing data on: {}".format(_filepath))
  writer_df.save(_filepath)

# COMMAND ----------

def download_to_transient(url,filepath,verify=True):
  """Download files from an url and save in a filepath

  Args:
      url (str): url to download data
      filepath (str): filepath to save data
      verify (Bool): Default to True. Verify or not the host do download
  """
  
  # Get url in stream mode
  with get(url,stream=True,verify=verify) as response:
    
    # Save in chunks
    with open(filepath,"wb") as download_file:
      for chunk in response.iter_content(chunk_size=1024 * 256):
        download_file.write(chunk)


def unzip_from_transient_to_bronze(filepath_zip,filepath_dst,extension_to_extract=None):
  """Unzip archive and save data

  Args:
      filepath_zip (str): filepath of zip file
      filepath_dst (str): filepath destination (place to save data)
      extension_to_extract (str): Default None. Extension to look in zip files. If None then extract all files
  """

  # Open zip
  with ZipFile(filepath_zip) as zip_file:
    
    # Get list of files and folders
    for zip_info in zip_file.infolist():
      
      # To skip folders
      if(zip_info.filename[-1] != '/'):
        
        # To not extract folder struct
        zip_info.filename = path.basename(zip_info.filename)
        
        # If None extract all files
        if(extension_to_extract is None):
          print("Extracting file {} to {}".format(zip_info.filename,filepath_dst))
          zip_file.extract(zip_info, filepath_dst)
        
        # Or if extension exist just extract files of these extension
        elif(zip_info.filename.lower().endswith(extension_to_extract)):
          print("Extracting file {} to {}".format(zip_info.filename,filepath_dst))
          zip_file.extract(zip_info, filepath_dst)
          
def move_file(src,dst):
  """Move file between two buckets with prefix

  Args:
      src (str): source bucket
      dst (str): dst bucket
  """
  
  # Guarantee destination have the correct pattern
  dst = sub(" ","_",sub("\s+"," ",dst)).lower()
  
  # Print
  print("Moving file from {} to {}".format(src,dst))
  
  dbutils.fs.mv(src,dst)
  
def get_size_from_filepath(filepath,size_type="b"):
  """Get the size from all files in a filepath

  Args:
      filepath (str): source bucket
      size_type (str): Type of conversion to apply (b is bytes, kb is killobytes, mb is megabytes and gb is gigabytes)
  """
  
  from numpy import sum as np_sum
  
  # Get sizes
  sizes = [file.size for file in dbutils.fs.ls(filepath)]
  
  # Sum all
  size_all = np_sum(sizes)
  
  # Check if need conversion
  if(size_type == "b"):
    pass
  elif(size_type == "kb"):
    size_all = size_all / (1024)
  elif(size_type == "mb"):
    size_all = size_all / (1024**2)
  elif(size_type == "gb"):
    size_all = size_all / (1024**3)
  else:
    raise ValueError("size_type '{}' its invalid. Only allowed values are:  'b', 'kb', 'mb' and 'gb'".format(size_type))
  
  # Sum
  return size_all

# COMMAND ----------

def get_files(file_regex,filepath):
  """Get file from filepath that match an regex

  Args:
      file_regex (str): Regex of file to match
      filepath (str): filepath to look for the file
  """
  
   # Files that match regex
  files_match = [file.path for file in dbutils.fs.ls(filepath) if search(file_regex,file.name)]
  
  return files_match

# COMMAND ----------

def build_case_when(dict_case_when,column,operator,alias):
  
  # Build case when
  select_case_when = ["WHEN {} {} '{}' THEN '{}'".format(column,operator,case,dict_case_when[case]) for case in dict_case_when]
  select_case_when = "CASE " + " ".join(select_case_when) + " ELSE NULL END AS " + alias
  select_case_when = f.expr(select_case_when)
  
  return select_case_when

# COMMAND ----------

def one_hot_encoding(df,column_name):

    # Get distinct values in column
    distinct_values = df.select(column_name).distinct().collect()

    # Matrix of distinct values for one hot encoding
    matrix = identity(len(distinct_values),dtype=int)

    # Build the first select to create an array with one hot for each distinct value
    select_one_hot = []
    for idx, value in enumerate(distinct_values):
      _ = "WHEN {} == '{}' THEN ".format(column_name,value[0]) + "array({})".format(", ".join(list(map(str,list(matrix[idx,:])))))
      select_one_hot.append(_)
    select_one_hot = f.expr("CASE " + " ".join(select_one_hot) + " END AS {}".format(column_name))
      
    # Name of array column
    column_name_array = "{}_ARRAY".format(column_name)

    # Apply select with one hot
    df = df.withColumn(column_name_array,select_one_hot)

    # Explode this array into new columns
    select_explode_one_hot = []
    for value in range(len(distinct_values)):
        select_explode_one_hot.append(f.col(column_name_array)[value].alias("{}_{}".format(column_name,value)))

    # Apply select explode
    df = df.select("*",*select_explode_one_hot).drop(column_name_array)

    return df

# COMMAND ----------

def schema2Table(df,table_name):
  
  # Generate a schema in table format
  data = []
  for s in df.schema:
    if(s.dataType == StringType()):
      data.append((s.name,"","string"))
    elif(s.dataType == IntegerType()):
      data.append((s.name,"","integer"))
    elif(s.dataType == DoubleType()):
      data.append((s.name,"","double"))
    elif(s.dataType == LongType()):
      data.append((s.name,"","long"))
    else:
      fields = df.schema[s.name].jsonValue()["type"]["fields"]
      for field in fields:
        data.append((s.name,field["name"],field["type"]))
  
  # Convert to spark DataFrame
  df = spark.createDataFrame(data,schema=["Coluna 1","Coluna 2","Tipo"])
  
  # Set table name
  df = df.select("*",f.lit(table_name).alias("Tabela"))
  
  # Fix position
  df = df.select(["Tabela","Coluna 1","Coluna 2","Tipo"])
        
  return df

# COMMAND ----------

def set_id(df,id_columns,seed,alias="ID"):

  # Cast to string
  select_id_columns = [f.col(column).cast("string") for column in id_columns]
  
  # Set seed
  select_id_columns = select_id_columns + [f.lit(seed)]
  
  # Fill nulls
  select_id_columns = f.concat(*[f.when(column.isNull(),"").otherwise(column) for column in select_id_columns])
  
  # Select hash
  select_hash = f.sha2(select_id_columns,256).alias(alias)

  # Apply hash
  df = df.select(select_hash,"*")
  
  return df

# COMMAND ----------

@udf(returnType=StringType())
def normalize_str(input_str):
    
    if(isinstance(input_str,str)):  
      nfkd_form = unicodedata_normalize('NFKD', input_str)
      return u"".join([c for c in nfkd_form if not unicodedata_combining(c)])
    else:
      return None
