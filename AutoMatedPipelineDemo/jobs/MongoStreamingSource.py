# Databricks notebook source
# MAGIC %sh
# MAGIC pip install dnspython==2.0.0

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install pymongo simplejson

# COMMAND ----------

# MAGIC %run /AutoMatedPipelineDemo/utils/adls_config

# COMMAND ----------

# MAGIC %run /AutoMatedPipelineDemo/utils/miscfunctions

# COMMAND ----------

import pymongo
import dns
import json
from bson.json_util import dumps
from pyspark.sql.types import *
from pyspark.sql.functions import from_json
from pyspark.sql.functions import json_tuple
from pyspark.sql.functions import col
from pyspark.sql.functions import get_json_object
from bson.objectid import ObjectId
import pyspark.sql.types as T
import pyspark.sql.functions as F

# COMMAND ----------

def initiate_mongo():
  client=pymongo.MongoClient(("mongodb+srv://admin:kDC7HUCt1lIhOKNh@democluster.vyvkl.mongodb.net/WideWorldImporters?retryWrites=true&w=majority"))
  database=client['WideWorldImporters']
  return database

# COMMAND ----------

def create_data_events(all_database):
  with all_database.watch() as stream:
    for events in stream:
      data_json=dumps(events)
      try:
        data=events['fullDocument']
      except:
        data = events['updateDescription']['updatedFields']
      documentKey = events['documentKey']['_id']
      table =events['ns']['coll']
      db =events['ns']['db']
      operation=events['operationType']
      query = {"_id": ObjectId(documentKey)}
      filter = {"_id": 0}
      collection = all_database[table]
      #print(collection)
      #print(collection.find_one(query, filter))
      stream_data = collection.find_one(query, filter)
      #stream_data={
      #"operation":operation,
      #"table":table,
      #"database" : db ,  
      #"data":data
      #  }
      put_data_autoloader(stream_data,table)
      resume_token=stream.resume_token

# COMMAND ----------

def put_data_autoloader(json_data , table):
  #print(json_data)
  print("=========================================")
  json_str = str(json_data)
  #datadictpath = "abfss://loaderlogic@demo343.dfs.core.windows.net/config/dataDict.csv"
  #incrpath = "abfss://loaderlogic@demo343.dfs.core.windows.net/WideWorldImporters/incremental/"+table+"/"
  datadictpath = "dbfs:/FileStore/dataDict.csv"
  incrpath = "dbfs:/data/bronze/incremental/"+table+"/"
  dataDictDF = spark.read.format("csv").option("header","true").load(datadictpath)
  dataDictDF.createOrReplaceTempView("dataDict")
  schemaDF = spark.sql("select dataDict.COLUMN_NAME || ' ' || dataType  ddl_schema_string  from dataDict  where  dataDict.TABLE_NAME = '" +table+"'")
  #schemaDF =schemaDF.sort(col("ORDINAL_POSITION"))
  ddl_schema = schemaDF.collect()
  ddl_schema_string = ''
  for x in ddl_schema:
        ddl_schema_string = ddl_schema_string + ',' + x[0]
  ddl_schema = T._parse_datatype_string(ddl_schema_string.lstrip(','))
  print(ddl_schema)
  print("============")
  x = json_str.replace("None", "null")
  y = x.replace("False", "false")
  z = y.replace("True", "true")
  #print(z)
  print("=========================================")
  #stud_obj = json.loads(z)
  #print(stud_obj)
  #json_obj = json.dumps(stud_obj)
  df = spark.read.schema(ddl_schema).option("multiline","true").json(sc.parallelize([z]))
  df.printSchema()
  #df.show(truncate=False)
  df = df.withColumn("part_date", F.current_date())
  df.printSchema()
  #df.coalesce(1).write.mode("overwrite").partitionBy("part_date").save(bronzepath)
  df.coalesce(1).write.mode("overwrite").partitionBy("part_date").parquet(incrpath)
  #df3 = spark.read.option("multiline","true").json(sc.parallelize([str(json_data)]))
  #df3.show(truncate=False)
  print('Data inserted in AutolOader')

# COMMAND ----------

all_database=initiate_mongo()
streamdata = create_data_events(all_database)
