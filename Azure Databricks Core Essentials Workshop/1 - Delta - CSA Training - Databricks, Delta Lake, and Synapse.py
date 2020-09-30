# Databricks notebook source
# MAGIC %md #### Use Databrick Runtime 6.x (Spark 2.4)

# COMMAND ----------

# DBTITLE 1,Clean-Up (run first)
dbutils.fs.rm('/fakeADLSG2/data/cdr/CDRDeltaBronze', True)
dbutils.fs.rm('/fakeADLSG2/data/cdr/CDRDeltaSilver', True)
dbutils.fs.rm('/fakeADLSG2/data/cdr/CDRDeltaGold', True)

# COMMAND ----------

# MAGIC %md
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/csaTraining20Q3/Architecture.jpg" alt="Architecture" width="700"> </br></br>
# MAGIC In this demo, a simple architecture that uses best-in-class tools for each part of the data flow will be </br> 
# MAGIC used.  ADLS G2 will be used for object/file storage, and the Delta Lake format will be used to store transactional, </br> 
# MAGIC curated data.  Azure Databricks will process the data from its raw format into different curated datasets before</br>
# MAGIC writing an aggregate dataset to a Provisioned SQL Pool in Azure Synapse.

# COMMAND ----------

# MAGIC %md
# MAGIC CDR Data Flow </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/csaTraining20Q3/dataflow.jpg" alt="Data Flow" width=750> </br></br>
# MAGIC The demo will walk through the general data flow illustrated above.

# COMMAND ----------

# MAGIC %md
# MAGIC ####COPY Raw Data into the Delta Lake Format </br>
# MAGIC Raw CSV files landing in ADLS G2 are copied into a Delta Lake location using the COPY INTO command. </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/csaTraining20Q3/bronze.jpg" alt="COPY into Delta" width="350">

# COMMAND ----------

# MAGIC %md Source Call Detail Record (CDR) Data

# COMMAND ----------

display(
  dbutils.fs.ls('wasbs://publicdata@mcg1stanstor00.blob.core.windows.net/cdr')
)

# COMMAND ----------

display(
  spark.read.csv('wasbs://publicdata@mcg1stanstor00.blob.core.windows.net/cdr', header=True, inferSchema=True)
)

# COMMAND ----------

# MAGIC %md Copy Raw Data into Delta Format

# COMMAND ----------

# MAGIC %sql
# MAGIC COPY INTO delta.`/fakeADLSG2/data/cdr/CDRDeltaBronze`
# MAGIC   FROM (SELECT * FROM 'wasbs://publicdata@mcg1stanstor00.blob.core.windows.net/cdr')
# MAGIC   FILEFORMAT = CSV
# MAGIC   FORMAT_OPTIONS('header' = 'true', 'inferSchema' = 'true')

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS CDRDeltaBronze;
# MAGIC CREATE TABLE CDRDeltaBronze
# MAGIC USING delta
# MAGIC LOCATION '/fakeADLSG2/data/cdr/CDRDeltaBronze'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CDRDeltaBronze

# COMMAND ----------

# MAGIC %md
# MAGIC ####Join/Transform Raw Delta Lake Records and Append to Curated CDR Records in Delta Lake Format </br>
# MAGIC Raw CDR Records are joined to a Country Code lookup table before being appended to an existing Curated CDR Delta Lake table. </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/csaTraining20Q3/silver.jpg" alt="Append to Curated" width="350">

# COMMAND ----------

# MAGIC %md Basic Transformation/Lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS mcCountryCodes;
# MAGIC CREATE TABLE IF NOT EXISTS mcCountryCodes (
# MAGIC   countryCode INT
# MAGIC   ,country STRING
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS(
# MAGIC   `header` 'true'
# MAGIC )
# MAGIC LOCATION 'wasbs://publicdata@mcg1stanstor00.blob.core.windows.net/countryCodes';
# MAGIC 
# MAGIC SELECT * FROM mcCountryCodes

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT cc.country, smsb.*
# MAGIC FROM CDRDeltaBronze smsb
# MAGIC INNER JOIN mcCountryCodes cc
# MAGIC   ON smsb.countryCode = cc.countryCode

# COMMAND ----------

# MAGIC %md Append to Curated Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table with some data already loaded
# MAGIC DROP TABLE IF EXISTS CDRDeltaSilver;
# MAGIC CREATE TABLE IF NOT EXISTS CDRDeltaSilver
# MAGIC USING delta
# MAGIC LOCATION '/fakeADLSG2/data/cdr/CDRDeltaSilver'
# MAGIC AS (
# MAGIC   SELECT cc.country, smsb.*
# MAGIC   FROM CDRDeltaBronze smsb
# MAGIC     INNER JOIN mcCountryCodes cc
# MAGIC       ON smsb.countryCode = cc.countryCode
# MAGIC   LIMIT 10
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO CDRDeltaSilver
# MAGIC SELECT cc.country, smsb.*
# MAGIC   FROM CDRDeltaBronze smsb
# MAGIC     INNER JOIN mcCountryCodes cc
# MAGIC       ON smsb.countryCode = cc.countryCode

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CDRDeltaSilver
# MAGIC WHERE Country IN ('USA', 'Germany', 'Belgium')

# COMMAND ----------

# MAGIC %md
# MAGIC ####Create Daily Aggregate and MERGE INTO Daily Aggregate Delta Lake Table </br>
# MAGIC Aggregate the Curated Delta data to Country, Day, Hour and MERGE the aggregates into an existing Delta Lake table. </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/csaTraining20Q3/gold.jpg" alt="Append to Curated" width="350">

# COMMAND ----------

# MAGIC %md Update Daily Aggregate Delta table with SQL MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   country
# MAGIC   ,datetime
# MAGIC   ,day(datetime) as day
# MAGIC   ,hour(datetime) as hour
# MAGIC   ,SUM(smsin) as smsin
# MAGIC   ,SUM(smsout) as smsout
# MAGIC   ,SUM(callin) as callin
# MAGIC   ,SUM(callout) as callout
# MAGIC   ,SUM(internet) as internet
# MAGIC FROM CDRDeltaSilver
# MAGIC GROUP BY 
# MAGIC   country, datetime, day, hour

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create target table with some data already loaded
# MAGIC DROP TABLE IF EXISTS CDRDeltaGold;
# MAGIC CREATE TABLE IF NOT EXISTS CDRDeltaGold
# MAGIC USING delta
# MAGIC LOCATION '/fakeADLSG2/data/cdr/CDRDeltaGold'
# MAGIC AS (
# MAGIC   SELECT
# MAGIC     country
# MAGIC     ,datetime
# MAGIC     ,day(datetime) as day
# MAGIC     ,hour(datetime) as hour
# MAGIC     ,SUM(smsin) as smsin
# MAGIC     ,SUM(smsout) as smsout
# MAGIC     ,SUM(callin) as callin
# MAGIC     ,SUM(callout) as callout
# MAGIC     ,SUM(internet) as internet
# MAGIC   FROM CDRDeltaSilver
# MAGIC   GROUP BY 
# MAGIC     country, datetime, day, hour
# MAGIC   LIMIT 10
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH source as (
# MAGIC SELECT
# MAGIC     country
# MAGIC     ,datetime
# MAGIC     ,day(datetime) as day
# MAGIC     ,hour(datetime) as hour
# MAGIC     ,SUM(smsin) as smsin
# MAGIC     ,SUM(smsout) as smsout
# MAGIC     ,SUM(callin) as callin
# MAGIC     ,SUM(callout) as callout
# MAGIC     ,SUM(internet) as internet
# MAGIC   FROM CDRDeltaSilver
# MAGIC   GROUP BY 
# MAGIC     country, datetime, day, hour
# MAGIC )
# MAGIC 
# MAGIC MERGE INTO CDRDeltaGold t
# MAGIC USING source s
# MAGIC ON s.datetime = t.datetime AND s.country = t.country
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CDRDeltaGold

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY CDRDeltaGold

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write Daily Aggregate to Synapse Provisioned SQL Pool </br>
# MAGIC Using the COPY command in the optimized Synapse connector, the Daily Aggregate data is loaded into a table in a Provisioned SQL Pool. </br>
# MAGIC <img src="https://mcg1stanstor00.blob.core.windows.net/images/demos/csaTraining20Q3/synapse.jpg" alt="Synapse" width="350"> </br>
# MAGIC 
# MAGIC *///This code will not be able to be executed without a Provisioned SQL Pool in Synapse///*
# MAGIC 
# MAGIC Docs: https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/synapse-analytics#sql

# COMMAND ----------

# DBTITLE 1,Fill below with your Synapse SQL Pool / Blob connection info
# MAGIC %sql 
# MAGIC -- Set up the Blob storage account access key in the notebook session conf.
# MAGIC SET fs.azure.account.key.<your-storage-account-name>.blob.core.windows.net=<your-storage-account-access-key>;
# MAGIC 
# MAGIC -- Read data using SQL.
# MAGIC CREATE TABLE SynCDRGold
# MAGIC USING com.databricks.spark.sqldw
# MAGIC OPTIONS (
# MAGIC   url 'jdbc:sqlserver://<the-rest-of-the-connection-string>',
# MAGIC   forwardSparkAzureStorageCredentials 'true',
# MAGIC   dbTable 'CDRGoldAgg',
# MAGIC   tempDir 'wasbs://<your-container-name>@<your-storage-account-name>.blob.core.windows.net/<your-directory-name>'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC SET fs.azure.account.key.YOUR_BLOG.blob.core.windows.net=##STORAGE ACCNT KEY##;
# MAGIC 
# MAGIC INSERT OVERWRITE TABLE SynCDRGold
# MAGIC SELECT * 
# MAGIC FROM CDRDeltaGold

# COMMAND ----------

