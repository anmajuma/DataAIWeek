# Databricks notebook source
dbutils.widgets.text("sourcesystem", "", "sourcesystem")
dbutils.widgets.text("sourcetable", "", "sourcetable")

# COMMAND ----------

sourcesystem = dbutils.widgets.get("sourcesystem")
sourcetable = dbutils.widgets.get("sourcetable")

# COMMAND ----------

# MAGIC %run /AutoMatedPipelineDemo/utils/adls_config

# COMMAND ----------

# MAGIC %run /AutoMatedPipelineDemo/utils/miscfunctions

# COMMAND ----------

from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import col
import pyspark.sql.functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

tableName = sourcetable
bronzepath = "dbfs:/data/bronze/"+tableName+"/"
silverpath = "dbfs:/data/silver/"+tableName+"/"
datadictpath = "dbfs:/FileStore/dataDict.csv"

# COMMAND ----------

dataDictDF = spark.read.format("csv").option("header","true").load(datadictpath)
dataDictDF.createOrReplaceTempView("datadict")
sourcePKDF= spark.sql("select COLUMN_NAME from datadict where PRIMARY_KEY = 'YES' and TABLE_NAME ='"+tableName+"'")
sourcePKrdd=sourcePKDF.collect()
print(sourcePKrdd)
pkList = list()
mergeClauseStr = ""
for x in sourcePKrdd:
        pkList.append(x[0])
        mergeClauseStr = mergeClauseStr + tableName+"."+x[0] + " = updates."+x[0] + " and"
        
print(pkList)
mergerClause = mergeClauseStr.rstrip("and")
print(mergerClause)

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
          .option("cloudFiles.format", "parquet")\
          .option("mergeSchema", "true")\
          .option("cloudFiles.validateOptions", "false")\
          .option("cloudFiles.schemaLocation", "dbfs:/data/schema/"+tableName+"/") \
          .option("checkpointLocation", "dbfs:/data/checkpoint/"+tableName+"/") \
          .load("dbfs:/data/bronze/incremental/"+tableName+"/")

# COMMAND ----------

# DBTITLE 1,Function for SCD Type1 in Silver Table
def upsert_data(df, epochId):
  
  #bronzepath = "abfss://loaderlogic@demo343.dfs.core.windows.net/WideWorldImporters/bronze/"+tableName+"/"
  #silverpath = "abfss://loaderlogic@demo343.dfs.core.windows.net/WideWorldImporters/silver/"+tableName+"/"
  bronzepath = "dbfs:/data/bronze/"+tableName+"/"
  silverpath = "dbfs:/data/silver/"+tableName+"/"
  df.write.mode("append").format("delta").save(bronzepath)
  brnzDF = df.drop("_rescued_data")
  brnzDF.show()
  ###### Header Check #######
  hdrchkstat= header_check(brnzDF,tableName,dataDictDF)
  
  ##### NOT NULL Check #####
  nullValidation(brnzDF, tableName , dataDictDF )
  
  ##### Data Type Check #####
  dataTypeValidation(brnzDF, tableName , dataDictDF)
  
  # Returns the current local date
  now = datetime.now() # current date and time
  today = now.strftime("%Y-%m-%d")
  print("date :",today)
  #sourceColumns = sourceColList(tableName)
  try:
      bdf1 = spark.sql("select * from error_"+sourcesystem+"."+tableName +" where part_date = '"+today+"'")
      bdf2 = bdf1.drop("error_desc")
      baddf = bdf2.drop("part_date")
      baddf.printSchema()
      brnzDF.printSchema()
      gooddf = brnzDF.subtract(baddf)
  except :
      gooddf = brnzDF
  gooddf.persist()
  cleansedDF = dateFormatStandardize(gooddf,tableName, dataDictDF)
  updateedRecordCount = cleansedDF.count()
  if  updateedRecordCount >  0 :
    
    deltaTableCities = DeltaTable.forPath(spark, silverpath)
    deltaTableCities.alias(tableName) \
      .merge(
        cleansedDF.alias('updates'),
        mergerClause
      ) \
      .whenMatchedUpdateAll() \
      .whenNotMatchedInsertAll() \
      .execute()
  else :
    print("No Valid Record For Update")
    sendErrorReport(bdf1, tableName)
    

# COMMAND ----------

# DBTITLE 1,Load data incrementally as it lands on the storage
df.writeStream\
  .format("delta")\
  .option("checkpointLocation", "dbfs:/data/checkpoint/"+tableName+"/")\
  .trigger(once=True)\
  .foreachBatch(upsert_data)\
  .option("mergeSchema", "true") \
  .start()
