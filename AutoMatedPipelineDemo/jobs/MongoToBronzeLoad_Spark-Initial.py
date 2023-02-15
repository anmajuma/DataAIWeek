# Databricks notebook source
# MAGIC %sh
# MAGIC pip install --upgrade 'pymongo[srv]'

# COMMAND ----------

# MAGIC %run /AutoMatedPipelineDemo/utils/adls_config

# COMMAND ----------

# MAGIC %run "/AutoMatedPipelineDemo/utils/miscfunctions"

# COMMAND ----------

def initiate_mongo():
    client=pymongo.MongoClient(("mongodb+srv://admin:kDC7HUCt1lIhOKNh@democluster.vyvkl.mongodb.net/WideWorldImporters?retryWrites=true&w=majority"))
    database=client['WideWorldImporters']
    return database

# COMMAND ----------

import pymongo
import json
from bson.json_util import dumps
import pandas as pd
from pandas import json_normalize
import pyspark.sql.types as T
from pyspark.sql.functions import col
from delta.tables import *
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *

# COMMAND ----------

my_spark = SparkSession \
    .builder \
    .appName("MongoDB") \
    .config("spark.mongodb.input.uri", "mongodb+srv://animesh:Halo343434@democluster-vyvkl.mongodb.net/WideWorldImporters?authSource=admin&replicaSet=DemoCluster-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true") \
    .config("spark.mongodb.output.uri", "mongodb+srv://animesh:Halo343434@democluster-vyvkl.mongodb.net/WideWorldImporters?authSource=admin&replicaSet=DemoCluster-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true") \
    .getOrCreate()

# COMMAND ----------

mongodb=initiate_mongo()
collectionList = mongodb.list_collection_names()
for x in collectionList:
    print(x)
    #bronzepath = "abfss://loaderlogic@demo343.dfs.core.windows.net/WideWorldImporters/bronze/"+x+"/"
    bronzepath = "dbfs:/data/bronze/"+x+"/"
    print(bronzepath)
    df = my_spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri","mongodb+srv://animesh:Halo343434@democluster-vyvkl.mongodb.net/WideWorldImporters."+x+"?authSource=admin&replicaSet=DemoCluster-shard-0&readPreference=primary&appname=MongoDB%20Compass&ssl=true").load()
    #df =df.withColumn('IngestTimeStamp', F.current_timestamp())
    df = df.withColumn("part_date", F.current_date())
    df=df.drop("_id")
    df.printSchema()
    df.write.mode("overwrite").partitionBy("part_date").save(bronzepath)
    sparkDF = spark.read.load(bronzepath)
    print("==============================================================")
    sparkDF.printSchema()
    print("***************************************************************")
    #for field in sparkDF.schema.fields:
    #    #print(field.name +" , "+str(field.dataType))
    #    pdf = pdf.append({'tablename' : x ,'fieldname' : field.name  , 'dataType' : str(field.dataType)},ignore_index = True)
    #    schemaDF = spark.createDataFrame(pdf)

