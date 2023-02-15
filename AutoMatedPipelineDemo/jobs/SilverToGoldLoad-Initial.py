# Databricks notebook source
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

goldpath = "abfss://loaderlogic@demo343.dfs.core.windows.net/WideWorldImporters/gold/"

# COMMAND ----------

sqlstmnt = """

SELECT  CityID 
      ,CityName as City
      , StateProvinceName 
      ,Location
      , CntryTb.CountryName
      ,CntryTb.Continent
      ,CntryTb.Region
      ,CntryTb.Subregion
      ,CntryTb.LatestRecordedPopulation  
   FROM WideWorldImporters.Cities_silver CityTb,
  WideWorldImporters.StateProvinces_silver StProvTb ,
  WideWorldImporters.Countries_silver CntryTb
  WHERE CityTb.StateProvinceID = StProvTb.StateProvinceID
  AND StProvTb.CountryID = CntryTb.CountryID;
  """
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Cities")

# COMMAND ----------

sqlstmnt = """

 SELECT c.CustomerID , 
 c.CustomerName, 
 bt.CustomerName as BillToCustomer, 
 cc.CustomerCategoryName,
bg.BuyingGroupName, 
p.FullName, 
c.DeliveryPostalCode
        FROM WideWorldImporters.Customers_silver AS c
        INNER JOIN WideWorldImporters.BuyingGroups_silver  bg
        ON c.BuyingGroupID = bg.BuyingGroupID
        INNER JOIN WideWorldImporters.CustomerCategories_silver  AS cc
        ON c.CustomerCategoryID = cc.CustomerCategoryID
        INNER JOIN WideWorldImporters.Customers_silver  bt
        ON c.BillToCustomerID = bt.CustomerID
        INNER JOIN WideWorldImporters.People_silver  p
        ON c.PrimaryContactPersonID = p.PersonID ;
     """   
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Customer")

# COMMAND ----------

sqlstmnt = """

SELECT p.PersonID as WWI_Employee_ID, p.FullName, p.PreferredName, p.IsSalesperson              
FROM WideWorldImporters.People_silver p 
where p.IsEmployee = true
"""
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Employee")


# COMMAND ----------

sqlstmnt = """

SELECT si.StockItemID, si.StockItemName, c.ColorName, spt.PackageTypeName as Selling_Package,
               bpt.PackageTypeName as Buying_Package, si.Brand, si.Size, si.LeadTimeDays, si.QuantityPerOuter,
               si.IsChillerStock, si.Barcode, si.UnitPrice, si.RecommendedRetailPrice,
               si.TypicalWeightPerUnit
        FROM WideWorldImporters.StockItems_silver AS si
        INNER JOIN WideWorldImporters.PackageTypes_silver spt
        ON si.UnitPackageID = spt.PackageTypeID
        INNER JOIN WideWorldImporters.PackageTypes_silver bpt
        ON si.OuterPackageID = bpt.PackageTypeID
        LEFT OUTER JOIN WideWorldImporters.Colors_silver c
        ON si.ColorID = c.ColorID;
        
"""
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/StockItem")        

# COMMAND ----------

sqlstmnt = """
SELECT s.SupplierID, s.SupplierName, sc.SupplierCategoryName, p.FullName, s.SupplierReference,
               s.PaymentDays, s.DeliveryPostalCode
        FROM WideWorldImporters.Suppliers_silver s
        INNER JOIN WideWorldImporters.SupplierCategories_silver sc
        ON s.SupplierCategoryID = sc.SupplierCategoryID
        INNER JOIN WideWorldImporters.People_silver  p
        ON s.PrimaryContactPersonID = p.PersonID

"""
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Supplier")   

# COMMAND ----------

# MAGIC  %sql
# MAGIC  
# MAGIC  --DIM TRANSACTION TYPES
# MAGIC  
# MAGIC     SELECT CAST(sit.TransactionOccurredWhen AS date) AS Date_Key,
# MAGIC            sit.StockItemTransactionID ,
# MAGIC            sit.InvoiceID ,
# MAGIC            sit.PurchaseOrderID ,
# MAGIC            CAST(sit.Quantity AS int) AS Quantity,
# MAGIC            sit.StockItemID ,
# MAGIC            sit.CustomerID ,
# MAGIC            sit.SupplierID ,
# MAGIC            sit.TransactionTypeID ,
# MAGIC            sit.TransactionOccurredWhen 
# MAGIC     FROM WideWorldImporters.StockItemTransactions_silver AS sit

# COMMAND ----------

sqlstmnt = """

SELECT     o.OrderDate ,
           ol.PickingCompletedWhen,
           o.OrderID ,
           o.BackorderOrderID ,
           ol.Description,
           pt.PackageTypeName AS Package,
           ol.Quantity ,
           ol.UnitPrice ,
           ol.TaxRate ,
           ROUND(ol.Quantity * ol.UnitPrice, 2) AS Total_Excluding_Tax,
           ROUND(ol.Quantity * ol.UnitPrice * ol.TaxRate / 100.0, 2) AS Tax_Amount,
           ROUND(ol.Quantity * ol.UnitPrice, 2) + ROUND(ol.Quantity * ol.UnitPrice * ol.TaxRate / 100.0, 2) AS Total_Including_Tax,
           c.DeliveryCityID ,
           c.CustomerID ,
           ol.StockItemID ,
           o.SalespersonPersonID ,
           o.PickedByPersonID ,
           CASE WHEN ol.LastEditedWhen > o.LastEditedWhen THEN ol.LastEditedWhen ELSE o.LastEditedWhen END AS Last_Modified_When
    FROM WideWorldImporters.Orders_silver AS o
    INNER JOIN WideWorldImporters.OrderLines_silver AS ol
    ON o.OrderID = ol.OrderID
    INNER JOIN WideWorldImporters.PackageTypes_silver AS pt
    ON ol.PackageTypeID = pt.PackageTypeID
    INNER JOIN WideWorldImporters.Customers_silver AS c
    ON c.CustomerID = o.CustomerID
 
 """
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Movement")  

# COMMAND ----------

sqlstmnt = """

SELECT     po.OrderDate ,
           po.PurchaseOrderID ,
           pol.OrderedOuters ,
           pol.OrderedOuters * si.QuantityPerOuter AS Ordered_Quantity,
           pol.ReceivedOuters ,
           pt.PackageTypeName AS Package,
           pol.IsOrderLineFinalized ,
           po.SupplierID ,
           pol.StockItemID ,
           CASE WHEN pol.LastEditedWhen > po.LastEditedWhen THEN pol.LastEditedWhen ELSE po.LastEditedWhen END AS Last_Modified_When
    FROM WideWorldImporters.PurchaseOrders_silver AS po
    INNER JOIN WideWorldImporters.PurchaseOrderLines_silver AS pol
    ON po.PurchaseOrderID = pol.PurchaseOrderID
    INNER JOIN WideWorldImporters.StockItems_silver AS si
    ON pol.StockItemID = si.StockItemID
    INNER JOIN WideWorldImporters.PackageTypes_silver AS pt
    ON pol.PackageTypeID = pt.PackageTypeID
    ORDER BY po.PurchaseOrderID;
    
"""
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Order")      

# COMMAND ----------

sqlstmnt = """

SELECT     po.OrderDate ,
           po.PurchaseOrderID ,
           pol.OrderedOuters ,
           pol.OrderedOuters * si.QuantityPerOuter AS Ordered_Quantity,
           pol.ReceivedOuters ,
           pt.PackageTypeName AS Package,
           pol.IsOrderLineFinalized ,
           po.SupplierID ,
           pol.StockItemID ,
           CASE WHEN pol.LastEditedWhen > po.LastEditedWhen THEN pol.LastEditedWhen ELSE po.LastEditedWhen END AS Last_Modified_When
    FROM WideWorldImporters.PurchaseOrders_silver AS po
    INNER JOIN WideWorldImporters.PurchaseOrderLines_silver AS pol
    ON po.PurchaseOrderID = pol.PurchaseOrderID
    INNER JOIN WideWorldImporters.StockItems_silver AS si
    ON pol.StockItemID = si.StockItemID
    INNER JOIN WideWorldImporters.PackageTypes_silver AS pt
    ON pol.PackageTypeID = pt.PackageTypeID
    ORDER BY po.PurchaseOrderID;
    
"""
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Purchase")      

# COMMAND ----------

sqlstmnt = """

SELECT      i.InvoiceDate ,
           i.ConfirmedDeliveryTime ,
           i.InvoiceID ,
           il.Description,
           pt.PackageTypeName AS Package,
           il.Quantity,
           il.UnitPrice ,
           il.TaxRate ,
           il.ExtendedPrice - il.TaxAmount AS Total_Excluding_Tax,
           il.TaxAmount ,
           il.LineProfit AS Profit,
           il.ExtendedPrice AS Total_Including_Tax,
           CASE WHEN si.IsChillerStock = 0 THEN il.Quantity ELSE 0 END AS Total_Dry_Items,
           CASE WHEN si.IsChillerStock <> 0 THEN il.Quantity ELSE 0 END AS Total_Chiller_Items,
           c.DeliveryCityID ,
           i.CustomerID ,
           i.BillToCustomerID ,
           il.StockItemID ,
           i.SalespersonPersonID ,
           CASE WHEN il.LastEditedWhen > i.LastEditedWhen THEN il.LastEditedWhen ELSE i.LastEditedWhen END AS Last_Modified_When
    FROM WideWorldImporters.Invoices_silver AS i
    INNER JOIN WideWorldImporters.InvoiceLines_silver AS il
    ON i.InvoiceID = il.InvoiceID
    INNER JOIN WideWorldImporters.StockItems_silver AS si
    ON il.StockItemID = si.StockItemID
    INNER JOIN WideWorldImporters.PackageTypes_silver AS pt
    ON il.PackageTypeID = pt.PackageTypeID
    INNER JOIN WideWorldImporters.Customers_silver AS c
    ON i.CustomerID = c.CustomerID
    INNER JOIN WideWorldImporters.Customers_silver AS bt
    ON i.BillToCustomerID = bt.CustomerID
    ORDER BY i.InvoiceID, il.InvoiceLineID;
    
"""
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Sales")         

# COMMAND ----------

sqlstmnt = """
    SELECT ct.TransactionDate ,
           ct.CustomerTransactionID ,
           CAST(NULL AS int) AS SupplierTransactionID,
           ct.InvoiceID ,
           CAST(NULL AS int) AS PurchaseOrderID,
           CAST(NULL AS string) AS SupplierInvoiceNumber,
           ct.AmountExcludingTax ,
           ct.TaxAmount ,
           ct.TransactionAmount ,
           ct.OutstandingBalance,
           ct.IsFinalized ,
           COALESCE(i.CustomerID, ct.CustomerID) AS CustomerID,
           ct.CustomerID AS BillToCustomerID,
           CAST(NULL AS int) AS SupplierID,
           ct.TransactionTypeID ,
           ct.PaymentMethodID ,
           ct.LastEditedWhen 
    FROM WideWorldImporters.CustomerTransactions_silver AS ct
    LEFT OUTER JOIN WideWorldImporters.Invoices_silver AS i
    ON ct.InvoiceID = i.InvoiceID

    UNION ALL

    SELECT st.TransactionDate ,
           CAST(NULL AS int) AS CustomerTransactionID,
           st.SupplierTransactionID ,
           CAST(NULL AS int) AS InvoiceID,
           st.PurchaseOrderID ,
           st.SupplierInvoiceNumber ,
           st.AmountExcludingTax ,
           st.TaxAmount ,
           st.TransactionAmount ,
           st.OutstandingBalance ,
           st.IsFinalized ,
           CAST(NULL AS int) AS CustomerID,
           CAST(NULL AS int) AS BillToCustomerID,
           st.SupplierID ,
           st.TransactionTypeID ,
           st.PaymentMethodID ,
           st.LastEditedWhen 
    FROM WideWorldImporters.SupplierTransactions_silver AS st

"""
goldDF = spark.sql(sqlstmnt)
display(goldDF)
goldDF.write.mode("overwrite").save(goldpath+"/Transactions")   

# COMMAND ----------

import pandas as pd

goldpath = "abfss://loaderlogic@demo343.dfs.core.windows.net/WideWorldImporters/gold/"
fileinfo = dbutils.fs.ls(goldpath)
fileinfoDF = spark.createDataFrame(fileinfo)
pdf = fileinfoDF.toPandas()
for index, row in pdf.iterrows():
    print(row['path'])
    print(row['name'].rstrip('/'))
    gold_delta_path = row['path'] 
    print(gold_delta_path)
    spark.sql("CREATE TABLE IF NOT EXISTS WideWorldImporters."+row['name'].rstrip('/') + "_gold USING DELTA LOCATION '" + gold_delta_path + "'")
