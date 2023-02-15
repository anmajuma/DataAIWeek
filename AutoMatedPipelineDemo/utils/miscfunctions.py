# Databricks notebook source
def open_mysql_conn():
  import mysql.connector

  mydb = mysql.connector.connect(
    host="hackathon.cq0xiuzh9tks.us-east-1.rds.amazonaws.com",
    user="admin",
    password="Killzone2",
    database="AdventureWorksDW"
  )
  return (mydb)

# COMMAND ----------

import pyspark.sql.types as T
def header_check(brnzDF, tableName, dataDictDF ):
    dataDictDF.createOrReplaceTempView("dataDict")
    #schemaDF = spark.sql("select dataDict.COLUMN_NAME || ' ' || dataType.DATA_TYPE  ddl_schema_string ,  CAST(ORDINAL_POSITION AS INT) ORDINAL_POSITION from dataDict JOIN dataType ON dataDict.DATA_TYPE = dataType.SRC_TYPE where  dataDict.TABLE_NAME = '" +tableName+"'")
    schemaDF = spark.sql("select dataDict.COLUMN_NAME ddl_schema_string ,  CAST(ORDINAL_POSITION AS INT) ORDINAL_POSITION from dataDict where  dataDict.TABLE_NAME = '" +tableName+"'")
    schemaDF =schemaDF.sort(col("ORDINAL_POSITION"))
    ddl_schema = schemaDF.collect()
    ddl_schema_string = ''
    for x in ddl_schema:
        ddl_schema_string = ddl_schema_string + ',' + x[0]

    ddl_schema = ddl_schema_string.lstrip(',') + ",part_date"
    ddl_header = list(ddl_schema.split(","))
    print(ddl_header)
    ddl_header.sort()
    #ddl_schema = T._parse_datatype_string(ddl_schema_string.lstrip(','))
    #df = spark.read.load(brnzpath).limit(1)
    dfTemp = brnzDF.drop("IngestTimeStamp")
    df = dfTemp.drop("_id")
    df.printSchema()
    delta_schema = df.columns
    delta_schema.sort()
    print(ddl_header)
    print("===========================================")
    print(delta_schema)
    
    if ddl_header == delta_schema:  
        return("The schema header and file headers are equal")  
    else:  
        return("The schema header and file header are not equal")

# COMMAND ----------

def sourceColList(tableName):
    dataDictDF.createOrReplaceTempView("dataDict")
    #schemaDF = spark.sql("select dataDict.COLUMN_NAME || ' ' || dataType.DEST_TYPE  ddl_schema_string ,  CAST(ORDINAL_POSITION AS INT) ORDINAL_POSITION from dataDict JOIN dataType ON dataDict.DATA_TYPE = dataType.SRC_TYPE where  dataDict.TABLE_NAME = '" +tableName+"'")
    
    schemaDF = spark.sql("select *  from error_WideWorldImporters."+tableName ).limit(1)
    ddl_schema = schemaDF.columns
    ddl_schema_string = ''
    for x in ddl_schema:
        ddl_schema_string = ddl_schema_string + ',' + x

    ddl_schema = ddl_schema_string.lstrip(',')
    print(ddl_schema)                   
    return (ddl_schema)

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan,when,count

def dataTypeValidation(brnzDF, tableName, dataDictDF) :
  
    brnzDF = brnzDF.na.fill(value=0)
    brnzDF = brnzDF.na.fill("")
    error_path = "abfss://loaderlogic@demo343.dfs.core.windows.net/WideWorldImporters/error/"
    brnzDF.createOrReplaceTempView("bronzetable")
    dataDictDF.createOrReplaceTempView("dataDict")
    df = spark.sql("select dataType dtype, dataDict.COLUMN_NAME as fieldname, dataDict.IS_NULLABLE, dataDict.PRIMARY_KEY, dataDict.ORDINAL_POSITION from dataDict where dataDict.TABLE_NAME = '" +tableName+"'")
    #df = spark.sql("select dataType dtype, dataDict.COLUMN_NAME as fieldname, dataDict.IS_NULLABLE, dataDict.PRIMARY_KEY from dataDict ")
    df.createOrReplaceTempView("df2")
    bronzeTablePKDF = spark.sql("select fieldname from df2 where PRIMARY_KEY = 'YES' ")
    bronzeTablePK = bronzeTablePKDF.collect()[0][0]
    qstr = "'select cast(' || fieldname || ' as '|| dtype||' ) dtchk , "+ bronzeTablePK +", '|| fieldname ||' from bronzetable'"
    sqldf = spark.sql(" select  " + qstr +"  sqlquery from df2")
    pdsqldf = sqldf.toPandas()
#print(pdsqldf)
    for index, row in pdsqldf.iterrows():
      query = row['sqlquery']
      print(query)
      try: 
        df = spark.sql(query)
        fldnm = df.columns[2]
        edf = df.filter("dtchk IS NULL")
        fldnm = df.columns[2]
        if edf.count() > 0:
          print("DataType Mismatch Found")       
          errordf = brnzDF.join(edf,bronzeTablePK).select(brnzDF['*'])
          errordf = errordf.withColumn("error_desc", F.lit("Data Type MisMatch for "+fldnm))
          errordf = errordf.withColumn("part_date", F.current_date())
          errordf.write.mode("append").format("delta").partitionBy("part_date").save(error_path+"/"+ tableName+"/")
          error_delta_path = error_path+"/"+ tableName+"/"    
          spark.sql("CREATE DATABASE IF NOT EXISTS error_WideWorldImporters")
          spark.sql("CREATE TABLE IF NOT EXISTS error_WideWorldImporters."+tableName + " USING DELTA LOCATION '" + error_delta_path + "'")
        else:
          print("DataType Mismatch Not Found")
      except:
          print("****")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan,when,count

def nullValidation(brnzDF, tableName, dataDictDF) :
    error_path = "dbfs:/data/error/"
    #dataDictDF = spark.read.format("csv").option("header","true").load(datadictpath)

    dataDictDF.createOrReplaceTempView("dataDict")
    colDF = spark.sql("select COLUMN_NAME from dataDict where IS_NULLABLE = 'NO' and DATA_TYPE <> 'bit' and TABLE_NAME = '" +tableName+"'" )
    col_rdd = colDF.collect()
    col_string = ''
    for s in col_rdd:
        col_string = col_string +',' + s[0]

    col_list = list(col_string.lstrip(',').split(","))
    print(col_list)
    df_Columns=col_list
    brnzDF = brnzDF.drop("IngestTimeStamp")
    brnzDF = brnzDF.drop("part_date")
    nullCheckDF = brnzDF.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                           )).alias(c)
                    for c in df_Columns])
    nullCheckDF.show()
    col_list = nullCheckDF.columns
    for c in col_list:
      rdd = nullCheckDF.select(col(c)).collect()
      for rw in rdd :
           if rw[0] == 1 :
                print (c)
                errordf = brnzDF.filter((col(c)== '') | (col(c).isNull()) | (col(c).contains('NULL')) | (col(c).contains('None')) )
                errordf = errordf.withColumn("error_desc", F.lit("NULL Check Failed for Column "+c))
                errordf = errordf.withColumn("part_date", F.current_date())
                errordf.write.mode("append").format("delta").partitionBy("part_date").save(error_path+"/"+ tableName+"/")
                error_delta_path = error_path+"/"+ tableName+"/"    
                spark.sql("CREATE DATABASE IF NOT EXISTS error_WideWorldImporters")
                spark.sql("CREATE TABLE IF NOT EXISTS error_WideWorldImporters."+tableName + " USING DELTA LOCATION '" + error_delta_path + "'")

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.functions import col,isnan,when,count ,lit
import pandas as pd
def dateFormatStandardize(brnzDF, tableName, dataDictDF ) :
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  dataDictDF.createOrReplaceTempView("dataDict")
 
  df = spark.sql("select dataDict.COLUMN_NAME as fieldname from dataDict where DATA_TYPE in ('datetime2','date') and dataDict.TABLE_NAME = '" +tableName+"'")



  pdf = df.toPandas()

  print(pdf)

  if len(pdf) > 0 :
    for index, row in pdf.iterrows():
    #if row["dtype"] == "date":
          print(row["fieldname"])
          bdf1=brnzDF.withColumn(row["fieldname"],F.to_timestamp(col(row["fieldname"])))
          bdf2 = bdf1.withColumn(row["fieldname"],F.date_format(col(row["fieldname"]),"MM-dd-YYYY HH:mm:ss"))
          brnzDF= bdf2


    return (bdf2)
  else:
    return (brnzDF)

# COMMAND ----------

import re
def flattenDataFrame(explodeDF):
    DFSchema = explodeDF.schema
    fields = DFSchema.fields
    fieldNames = DFSchema.fieldNames()
    fieldLength = len(fieldNames)

    for i in range(fieldLength):
        field = fields[i]
        fieldName = field.name
        fieldDataType = field.dataType

        if isinstance(fieldDataType, ArrayType):
            fieldNameExcludingArray = list(filter(lambda colName: colName != fieldName, fieldNames))
            fieldNamesAndExplode = fieldNameExcludingArray + ["posexplode_outer({0}) as ({1}, {2})".format(fieldName, fieldName+"_pos", fieldName)]
            arrayDF = explodeDF.selectExpr(*fieldNamesAndExplode)
            return flattenDataFrame(arrayDF)
        elif isinstance(fieldDataType, StructType):
            childFieldnames = fieldDataType.names
            structFieldNames = list(map(lambda childname: fieldName +"."+childname, childFieldnames))
            newFieldNames = list(filter(lambda colName: colName != fieldName, fieldNames)) + structFieldNames
            renamedCols = map(lambda x: re.sub(r'[@|?|(|)|!|.|/|:]',r'_',x), newFieldNames)
            zipAliasColNames = zip(newFieldNames, renamedCols)
            aliasColNames = map(lambda y: col(y[0]).alias(y[1]), zipAliasColNames)
            structDF = explodeDF.select(*aliasColNames)
            return flattenDataFrame(structDF)
    return explodeDF

# COMMAND ----------

#from pyspark.sql import Row
#from pyspark.sql.functions import *
#import pandas as pd
#from IPython.display import HTML
#from pretty_html_table import build_table
#import os
#from sendgrid import SendGridAPIClient
#from sendgrid.helpers.mail import Mail

#def sendErrorReport(df, tablename) :

  #pandasDF = df.toPandas()
  #output = build_table(pandasDF, 'blue_light')
  #message = Mail(
  #   from_email='majumder.animesh@gmail.com',
  #   to_emails='anmajum@live.com',
  #   subject='Sending Data Ingestion Error Report for ' + tablename,
  #   html_content=output)
  #try:
  #   sg = SendGridAPIClient('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')
  #   response = sg.send(message)
  #   print(response.status_code)
  #   print(response.body)
  #   print(response.headers)
  #except Exception as e:
  #   print(e)

