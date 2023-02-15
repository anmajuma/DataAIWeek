# Databricks notebook source
dbutils.widgets.text("sourcesystem", "", "sourcesystem")
dbutils.widgets.text("sourcetable", "", "sourcetable")

# COMMAND ----------

sourcesystem = dbutils.widgets.get("sourcesystem")
sourcetable = dbutils.widgets.get("sourcetable")

# COMMAND ----------

import pyspark
from pyspark import SparkContext,SparkConf
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as F 

from delta.tables import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

# COMMAND ----------

# MAGIC %run /AutoMatedPipelineDemo/utils/adls_config

# COMMAND ----------

# MAGIC %run /AutoMatedPipelineDemo/utils/miscfunctions

# COMMAND ----------

brnzpath = "dbfs:/data/bronze/"+sourcetable+"/"
silverpath = "dbfs:/data/silver/"+sourcetable+"/"
errorpath = "dbfs:/data/error/"
datadictpath = "dbfs:/FileStore/dataDict.csv"

# COMMAND ----------

brnzDF = spark.read.load(brnzpath)
brnzDF.createOrReplaceTempView("bronzetable")
brnzDF.printSchema()
dataDictDF = spark.read.format("csv").option("header","true").load(datadictpath)

# COMMAND ----------

display(dataDictDF)

# COMMAND ----------

###### Header Check #######
hdrchkstat= header_check(brnzDF,sourcetable,dataDictDF)

if hdrchkstat == "The schema header and file headers are equal" :
  print("Header Match")
else :
  print("Header MisMatch")

# COMMAND ----------

##### NOT NULL Check #####
nullValidation(brnzDF, sourcetable , dataDictDF )

# COMMAND ----------

##### Data Type Check #####
dataTypeValidation(brnzDF, sourcetable , dataDictDF)

# COMMAND ----------

from datetime import datetime
    
# Returns the current local date
now = datetime.now() # current date and time
today = now.strftime("%Y-%m-%d")
print("date :",today)

try:
  sourceColumns = sourceColList(sourcetable)
  baddf = spark.sql("select "+sourceColumns+",IngestTimeStamp from error_"+sourcesystem+"."+sourcetable +" where part_date = '"+today+"'")
  baddf.printSchema()
  brnzDF.printSchema()
  gooddf = brnzDF.subtract(baddf)
except :
  gooddf = brnzDF
  
display(gooddf)

# COMMAND ----------

gooddf.persist()

# COMMAND ----------

cleansedDF = dateFormatStandardize(gooddf,sourcetable, dataDictDF)

# COMMAND ----------

display(cleansedDF)

# COMMAND ----------

##### Write To Silver Layer #####

cleansedDF = cleansedDF.withColumn("part_date", F.current_date())
cleansedDF.write.mode("overwrite").format("delta").partitionBy("part_date").save(silverpath)


spark.sql("CREATE DATABASE IF NOT EXISTS "+sourcesystem)
spark.sql("CREATE TABLE IF NOT EXISTS " + sourcesystem+"." +sourcetable + "_silver USING DELTA LOCATION '" + silverpath + "'")
