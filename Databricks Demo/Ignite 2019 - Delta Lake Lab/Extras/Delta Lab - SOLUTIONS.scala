// Databricks notebook source
// MAGIC %md
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/DeltaLake.png?raw=true" alt="drawing" width="960"/>

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC # About this Tutorial #  
// MAGIC The goal of this tutorial is to help you understand about Databricks Delta. Databricks Delta extends Apache Spark to simplify data reliability and boost Spark's performance. Building robust, high performance data pipelines can be difficult due to: lack of indexing and statistics, data inconsistencies introduced by schema changes and pipeline failures, and having to trade off between batch and stream processing. Databricks Delta is a Unified Data Management System that provides the following additional functionalities to tackle these issues.
// MAGIC  - **Enables Fast Queries at Massive Scale**
// MAGIC  
// MAGIC  Delta automatically indexes, compacts and caches data helping achieve up to 100x improved performance over Apache Spark. Delta delivers performance optimizations by automatically capturing statistics and applying various techniques to data for efficient querying.
// MAGIC  - **Makes Data Reliable for Analytics**
// MAGIC  
// MAGIC  Delta provide full ACID-compliant transactions and enforce schema on write, giving data teams controls to ensure data reliability. Deltaʼs upsert capability
// MAGIC provides a simple way to clean data and apply new business logic without reprocessing data.
// MAGIC  - **Simplifies Data Engineering**
// MAGIC  
// MAGIC  Delta dramatically simplifies data pipelines by providing a common API to transactionally store large historical and streaming datasets in cloud storage and making these massive datasets available for high-performance analytics.
// MAGIC  - **Natively Integrates with the Unified Analytics Platform**
// MAGIC  
// MAGIC  Databricks Delta, a key component of Databricks Runtime, enables data scientists to explore and visualize data and combine this data with various ML frameworks (Tensorflow, Keras, Scikit-Learn etc) seamlessly to build models. As a result, Delta can be used to run not only SQL queries but also for Machine Learning using Databricks Workspace on large amounts of streaming data.
// MAGIC 
// MAGIC  
// MAGIC  ##### We will use the publicly available IoT_Devices dataset built into DBFS: "dbfs:/databricks-datasets/iot/iot_devices.json". 
// MAGIC  
// MAGIC This tutorial covers the following tasks:
// MAGIC - Databricks Delta Introduction - Parquet format & Transaction Logs
// MAGIC - Operations on Delta Tables - Create, Read, Append data
// MAGIC - Table Metadata - Table History, Table Details
// MAGIC - ACID Properties Support - Updates and Upserts
// MAGIC - High performance Spark queries - Compaction, ZOrdering,
// MAGIC - Guidelines and Documentation

// COMMAND ----------

// MAGIC %md
// MAGIC #### We are going to use a dataset of IoT sensor readings
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/photo-power-plant.jpg?raw=true" alt="drawing" width="600"/>

// COMMAND ----------

// MAGIC %md
// MAGIC #### First lets take a peek at this data:

// COMMAND ----------

dbutils.fs.head("dbfs:/databricks-datasets/iot/iot_devices.json")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now we can read it in as a dataframe:

// COMMAND ----------

// DBTITLE 0,Load Sample Data
val rawPath = "dbfs:/databricks-datasets/iot/iot_devices.json"

val deviceRaw = spark.read.json(rawPath)
display(deviceRaw)

// COMMAND ----------

// MAGIC %md ### Create Table ###
// MAGIC 
// MAGIC Let us create a Delta Table using the records we have read in. You can use existing Spark SQL code and change the format from parquet, csv, json, and so on, to delta.

// COMMAND ----------

//write data to Delta Lake file
deviceRaw.write.format("delta").mode("overwrite").save("/delta/rawDevices")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Let's explore exactly what files were written in that transaction:

// COMMAND ----------

// MAGIC %fs ls "/delta/rawDevices"

// COMMAND ----------

// MAGIC %md
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/DeltaLake-TransactionLog1.png?raw=true" alt="drawing" width="600"/>
// MAGIC <img src="https://github.com/kywe665/HelpImageHost/blob/master/DeltaLake-TransactionLog2.png?raw=true" alt="drawing" width="600"/>

// COMMAND ----------

// MAGIC %md
// MAGIC #### Create a Databricks Delta table directly from the files

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS deviceRaw;
// MAGIC CREATE TABLE deviceRaw
// MAGIC USING delta
// MAGIC AS SELECT *
// MAGIC FROM delta.`/delta/rawDevices`

// COMMAND ----------

// MAGIC %md
// MAGIC ####Read the table
// MAGIC Explore the details and extra features that a Delta Table provides

// COMMAND ----------

// USING TABLE NAME
val raw_Delta_Table = spark.table("deviceRaw")

display(raw_Delta_Table)

// COMMAND ----------

// MAGIC %md 
// MAGIC Once our dataframe from Delta source is generated, we can click on the dataframe name in the notebook state panel to view the metadata related to our delta table. The metadata includes:
// MAGIC 
// MAGIC 
// MAGIC 1. Schema
// MAGIC 
// MAGIC ![schema](https://i.imgur.com/3pTqz6P.png)
// MAGIC 
// MAGIC 2. Details
// MAGIC 
// MAGIC ![details](https://i.imgur.com/9rbBfNr.png)
// MAGIC 
// MAGIC 3. History
// MAGIC 
// MAGIC ![history](https://i.imgur.com/us1a1FU.png)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Append data to a table ###
// MAGIC 
// MAGIC As new events arrive, you can atomically append them to the table. Let us create a new record that we will append to our table.

// COMMAND ----------

import org.apache.spark.sql.types._

val newRecord = Seq((8,
                     867L,
                     "US",
                     "USA",
                     "United States",
                     9787236987276352L,
                     "meter-gauge-1xbYRYcjdummy",
                     51L,
                     "68.161.225.1",
                     38.000000,
                     "green",
                     -97.000000,
                     "Celsius",
                     34L,
                     1458444054093L
                    ))
.toDF("battery_level", "c02_level", "cca2", "cca3", "cn", "device_id", "device_name", "humidity", "ip", "latitude", "lcd", "longitude", "scale", "temp", "timestamp")

display(newRecord)

// COMMAND ----------

// MAGIC %md
// MAGIC Append this record directly into our delta table

// COMMAND ----------

newRecord
  .write
  .format("delta")
  .mode("append")
  .saveAsTable("deviceRaw")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Failed Schema Merge? THANK YOU DELTA for enforcing the schema!!
// MAGIC Repeat below after correcting the schema. 
// MAGIC 
// MAGIC You can also tell Delta to merge schemas, or overwrite schemas, but it prevents unintentional schema changes or type mismatches

// COMMAND ----------

import org.apache.spark.sql.types._

val newRecord = Seq((8L,
                     867L,
                     "US",
                     "USA",
                     "United States",
                     9787236987276352L,
                     "meter-gauge-1xbYRYcjdummy",
                     51L,
                     "68.161.225.1",
                     38.000000,
                     "green",
                     -97.000000,
                     "Celsius",
                     34L,
                     1458444054093L
                    ))
.toDF("battery_level", "c02_level", "cca2", "cca3", "cn", "device_id", "device_name", "humidity", "ip", "latitude", "lcd", "longitude", "scale", "temp", "timestamp")

display(newRecord)

// COMMAND ----------

newRecord
  .write
  .format("delta")
  .mode("append")
  .saveAsTable("deviceRaw")

// COMMAND ----------

// MAGIC %md We can do the same thing using SQL

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO deviceRaw VALUES (8L, 867L, "US", "USA", "United States", 1787236987276352L, "meter-gauge-1xbYRYcjdummy", 51L, "68.161.225.1", 38.000000, "green", -97.000000, "Celsius", 34L, 1458444054093L)

// COMMAND ----------

// MAGIC %md
// MAGIC Let us perform a count operation on the delta table to see the updated count after append operation.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT count(*) FROM deviceRaw

// COMMAND ----------

// MAGIC %md 
// MAGIC ##DATABRICKS DELTA WITH STRUCTURED STREAMING##
// MAGIC Databricks Delta is deeply integrated with Spark Structured Streaming through readStream and writeStream. Databricks Delta overcomes many of the limitations typically associated with streaming systems and files, including:
// MAGIC 
// MAGIC    - Coalescing small files produced by low latency ingest.
// MAGIC    - Maintaining “exactly-once” processing with more than one stream (or concurrent batch jobs).
// MAGIC    - Efficiently discovering which files are new when using files as the source for a stream.

// COMMAND ----------

dbutils.fs.rm("/delta/_checkpoint",true)
dbutils.fs.rm("/delta/streamingDevices",true)

val newData = spark.readStream.table("deviceRaw")

newData.writeStream
  .format("delta")
  .outputMode("append")
  .option("checkpointLocation", "/delta/_checkpoint")
  .option("ignoreDeletes", true)
  .option("ignoreChanges", true)
  .start("/delta/streamingDevices/")

// COMMAND ----------

// MAGIC %md Structured Streaming does not handle input that is not an append and throws an exception if any modifications occur on the table being used as a source. There are two main strategies for dealing with changes that cannot be propagated downstream automatically:
// MAGIC 
// MAGIC    1. Since Databricks Delta tables retain all history by default, in many cases you can delete the output and checkpoint and restart the stream from the beginning.
// MAGIC    2. You can set either of these two options:
// MAGIC         -  `ignoreDeletes` skips any transaction that deletes entire partitions from the source table. For example, you can set this option if you delete data from the source table that was previously kept for data retention purposes and you do not want these deletions to propagate downstream.
// MAGIC         - `ignoreChanges` skips any transaction that updates data in the source table. For example, you can set this option if you use UPDATE, MERGE, DELETE, or OVERWRITE and these changes do not need to be propagated downstream.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Observe the stream processing

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

display(
  spark.readStream.format("delta")
  .load("/delta/streamingDevices/")
  .groupBy($"cn")
  .agg(sum($"battery_level").as("battery_level"))
  .orderBy($"battery_level".desc)
 )

// COMMAND ----------

// MAGIC %md
// MAGIC #### Add rows and see them get processed right away

// COMMAND ----------

val newRecord = Seq((999999L,
                     86L,
                     "NA",
                     "NAN",
                     "Unknown",
                     9787236987276352L,
                     "HELLO WORLD STREAM",
                     5L,
                     "68.161.225.1",
                     0.000000,
                     "test",
                     0.000000,
                     "Kelvin",
                     34L,
                     1458444054093L
                    ))
.toDF("battery_level", "c02_level", "cca2", "cca3", "cn", "device_id", "device_name", "humidity", "ip", "latitude", "lcd", "longitude", "scale", "temp", "timestamp")

newRecord
  .write
  .format("delta")
  .mode("append")
  .saveAsTable("deviceRaw")

// COMMAND ----------

// MAGIC %md
// MAGIC ##UPSERTS AND  UPDATES##
// MAGIC Unlike the file APIs in Apache Spark, Databricks Delta remembers and enforces the schema of a table. This means that by default overwrites do not replace the schema of an existing table.

// COMMAND ----------

// MAGIC %md
// MAGIC #### You are analyzing data and you notice that the US c02_levels are abnormally high above the rest

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT c02_level, deviceRaw.cn, timestamp
// MAGIC FROM deviceRaw 
// MAGIC WHERE cn IN ("United States", "United Kingdom", "China", "Japan", "Brazil")

// COMMAND ----------

// MAGIC %md
// MAGIC #### After discussing in depth with engineers responsible for the IoT sensors themselves you discover that the devices in US were configured to report c02_levels as milligram per cubic meter, instead of reporting as parts per million. 
// MAGIC Because of Avogadros number and PV=nRT, you can adjust these levels back to normal by dividing by 1.8. (*Not Acurate Science, just roleplay*) https://www.co2meter.com/blogs/news/15164297-co2-gas-concentration-defined

// COMMAND ----------

// MAGIC %sql
// MAGIC UPDATE deviceRaw SET c02_level = c02_level/1.8 WHERE cn = 'United States'

// COMMAND ----------

// MAGIC %md
// MAGIC #### Looks like everything is corrected back to normal now:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT c02_level, deviceRaw.cn, timestamp
// MAGIC FROM deviceRaw 
// MAGIC WHERE cn IN ("United States", "United Kingdom", "China", "Japan", "Brazil")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Let's say you work at a large corporation and you own a centralized data platform that 100's of employees around the globe are using. 
// MAGIC Let's say that you didn't communicate this change very well to all those who were consuming this data. Thank you Delta for audit history:

// COMMAND ----------

// MAGIC %sql
// MAGIC DESCRIBE HISTORY deviceRaw

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now your data science team tracks you down from Delta audit history and is mad. 
// MAGIC They have an ML Model deployed that predicts c02 levels and decides when to shut down certain plant operations based on those predictions. They were tracking their model accuracy via Azure Machine Learning and around the time you made changes to the data, their models started performing poorly. They have now learned this was because of your changes and are mad because now they have to build new models. They have to start training from scratch and they don't have any reproducible baselines to compare with because you already changed the data at the source.
// MAGIC 
// MAGIC #### Luckily with TIME TRAVEL you can easily create a view for your Data Science team to replay the data exactly how it was previously.

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE OR REPLACE TEMPORARY VIEW deviceRaw_PreUpdate
// MAGIC   AS 
// MAGIC   SELECT * FROM deviceRaw TIMESTAMP AS OF '2019-10-31T05:11:54.000+0000' --MAKE SURE TO COPY/PASTE Relevant timestamp here
// MAGIC   WHERE cn IN ("United States", "United Kingdom", "China", "Japan", "Brazil")
// MAGIC   ; 
// MAGIC 
// MAGIC SELECT * FROM deviceRaw_PreUpdate

// COMMAND ----------

// MAGIC %md ###UPSERTS (Merge Into) ###
// MAGIC 
// MAGIC The MERGE INTO statement allows you to merge a set of updates and insertions into an existing dataset. For example, the following statement takes a stream of updates and merges it into the trip_data_delta table. When there is already an event present with the same primary key containing , Databricks Delta updates the data column using the given expression. When there is no matching event, Databricks Delta adds a new row.

// COMMAND ----------

// MAGIC %md
// MAGIC #### To setup this part of lab, we are now creating a Battery status table where each row is a unique device

// COMMAND ----------

val deviceBatteryStatus = spark.sql("""
  SELECT battery_level, ip, deviceRaw.device_name 
  FROM deviceRaw
  INNER JOIN (
    SELECT device_name, MAX(timestamp) AS timestamp FROM deviceRaw GROUP BY device_name
  ) latest ON deviceRaw.device_name = latest.device_name AND deviceRaw.timestamp = latest.timestamp
""")

deviceBatteryStatus.write.format("delta").mode("overwrite").save("/delta/batteryStatus")

display(deviceBatteryStatus)

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS batteryStatus;
// MAGIC CREATE TABLE batteryStatus
// MAGIC USING delta
// MAGIC AS SELECT *
// MAGIC FROM delta.`/delta/batteryStatus`

// COMMAND ----------

// MAGIC %md
// MAGIC #### Now say we are streaming in new data and we need to update our battery status table so IT OPs knows when/where to change batteries

// COMMAND ----------

Seq(
  (3, "127.0.0.0", "A Hello World"),
  (-20, "9.8.8.8", "A Hello World 2")
)
.toDF("battery_level", "ip", "device_name").createOrReplaceTempView("newData")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM newData

// COMMAND ----------

// MAGIC %md
// MAGIC #### With simple MERGE commands we can update existing records and insert new records

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO batteryStatus
// MAGIC USING newData
// MAGIC ON batteryStatus.device_name = newData.device_name 
// MAGIC WHEN MATCHED THEN
// MAGIC   UPDATE SET
// MAGIC      batteryStatus.battery_level = newData.battery_level 
// MAGIC     ,batteryStatus.ip = newData.ip
// MAGIC WHEN NOT MATCHED
// MAGIC   THEN INSERT *

// COMMAND ----------

// MAGIC %md
// MAGIC #### Let us peek at the new rows that got inserted.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM batteryStatus ORDER BY device_name

// COMMAND ----------

// MAGIC %md
// MAGIC # GDPR
// MAGIC Now imagine that these devices somehow have the right to be forgotten and are protected under GDPR regulation (*Just Roleplay*)
// MAGIC 
// MAGIC There is a webapp where users can issue delete requests. The backend logs and sends all the delete requests to a single table. We now need to take this table and process the Delete requests.

// COMMAND ----------

//create some deleteRequest data
Seq(
  ("A Hello World"),
  ("A Hello World 2"),
  ("device-mac-100011QxHwwVgOra"),
  ("device-mac-100041lQOYr")
)
.toDF("device_name").createOrReplaceTempView("deleteRequests")

display(spark.sql("select * from deleteRequests"))

// COMMAND ----------

// MAGIC %md ###Delete###
// MAGIC 
// MAGIC As Delta Table maintains history of table versions, it supports deletion of records which cannot be done in normal SparkSQL Tables.

// COMMAND ----------

// MAGIC %sql
// MAGIC DELETE FROM batteryStatus WHERE device_name IN (
// MAGIC   SELECT DISTINCT device_name FROM deleteRequests
// MAGIC );

// COMMAND ----------

// MAGIC %md
// MAGIC #### OR MERGE

// COMMAND ----------

// MAGIC %sql
// MAGIC MERGE INTO batteryStatus
// MAGIC USING deleteRequests
// MAGIC ON batteryStatus.device_name = deleteRequests.device_name 
// MAGIC WHEN MATCHED THEN
// MAGIC   DELETE

// COMMAND ----------

// MAGIC %md Lets take a peek after delete operation.

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * FROM batteryStatus ORDER BY device_name

// COMMAND ----------

// MAGIC %md #Optimizing Performance and Cost#
// MAGIC 
// MAGIC To improve query speed, Databricks Delta supports the ability to optimize the layout of data stored in DBFS. Databricks Delta supports two layout algorithms: 
// MAGIC 
// MAGIC 1. bin-packing 
// MAGIC 2. ZOrdering.
// MAGIC 
// MAGIC This topic describes how to run the optimization commands and how the two layout algorithms work.
// MAGIC 
// MAGIC This topic also describes how to clean up stale table snapshots.
// MAGIC 
// MAGIC Further information about Optimizing Performance and Cost in Delta can be found [here](https://docs.databricks.com/delta/optimizations.html#frequently-asked-questions-faq)

// COMMAND ----------

// MAGIC %md
// MAGIC ###Compaction###
// MAGIC ####Optimize a table####
// MAGIC Once you have been streaming for awhile, you will likely have a lot of small files in the table. If you want to improve the speed of read queries, you can use OPTIMIZE to collapse small files into larger ones:
// MAGIC 1. Readers of Databricks Delta tables use snapshot isolation, which means that they are not interrupted when OPTIMIZE removes unnecessary files from the transaction log. OPTIMIZE makes no data related changes to the table, so a read before and after an OPTIMIZE has the same results. Performing OPTIMIZE on a table that is a streaming source does not affect any current or future streams that treat this table as a source.
// MAGIC 2. Bin-packing optimization is idempotent, meaning that if it is run twice on the same dataset, the second instance has no effect.
// MAGIC 3. Bin-packing aims to produce evenly-balanced data files with respect to their size on disk, but not necessarily number of tuples per file. However, the two measures are most often correlated.
// MAGIC 4. Bin-packing optimization is idempotent, meaning that if it is run twice on the same dataset, the second instance has no effect.
// MAGIC 
// MAGIC If you have a large amount of data and only want to optimize a subset of it, you can specify an optional partition predicate using `WHERE`:
// MAGIC `Only the partition columns may be referenced`
// MAGIC - Readers of Databricks Delta tables use snapshot isolation, which means that they are not interrupted when OPTIMIZE removes unnecessary files from the transaction log. OPTIMIZE makes no data related changes to the table, so a read before and after an OPTIMIZE has the same results. Performing OPTIMIZE on a table that is a streaming source does not affect any current or future streams that treat this table as a source.

// COMMAND ----------

// MAGIC %md
// MAGIC #####ZORDER#####
// MAGIC ZOrdering is a technique to colocate related information in the same set of files. This co-locality is automatically used by Databricks Delta data-skipping algorithms to dramatically reduce the amount of data that needs to be read. To ZOrder data, you specify the columns to order on in the ZORDER BY clause:

// COMMAND ----------

// MAGIC %md
// MAGIC OPTIMIZE consolidates files and orders the Databricks Delta table `trip_data_delta` data by `tpep_pickup_datetime` under each partition for faster retrieval

// COMMAND ----------

// MAGIC %md ##Let's See How Databricks Delta Makes Spark Queries Faster##
// MAGIC In this example, we will see how Databricks Delta can optimize query performance. We create a standard table using Parquet format and run a quick query to observe its latency. We then run a second query over the Databricks Delta version of the same table to see the performance difference between standard tables versus Databricks Delta tables.
// MAGIC 
// MAGIC Simply follow these 4 steps below:
// MAGIC 
// MAGIC - Step 1 : Create a standard Parquet based table using data from New York Taxi Dataset
// MAGIC - Step 2 : Run a query to find out the total fare earned by the taxis for each pickup location and get top 10 locations by fare.
// MAGIC - Step 3 : Create the flights table using Databricks Delta and optimize the table.
// MAGIC - Step 4 : Rerun the query in Step 2 and observe the latency.
// MAGIC 
// MAGIC Note: *Throughout the example we will be building few tables with a 10s of million rows. Some of the operations may take a few minutes depending on your cluster configuration.*

// COMMAND ----------

// MAGIC %md Read NYC Taxi Dataset from Azure Blob.

// COMMAND ----------

val storageAccountName = "taxinydata"
val containerName = "taxidata"
val storageAccountAccessKey = "tQe7Y/4XOeMmGcSOlAX3HMk3MSDxPzz80fGnX77VQev5jq9ObJGVa0wZvoerbZ2ozDmK9bsF/3cMs/K/asYCWg=="
spark.conf.set(s"fs.azure.account.key.${storageAccountName}.blob.core.windows.net", storageAccountAccessKey)
val path = s"wasbs://$containerName@$storageAccountName.blob.core.windows.net/yellow_tripdata_2017-01.csv"

val cabs = spark.read.option("header", true).csv(path)

// COMMAND ----------

// MAGIC %md Write parquet based table using cabs data

// COMMAND ----------

cabs.write.format("parquet").mode("overwrite").partitionBy("PULocationID").save("/tmp/cabs_parquet")

// COMMAND ----------

// MAGIC %md Next, we run a query that get top 10 Pickup Locations with highest fares.

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val cabs_parquet = spark.read.format("parquet").load("/tmp/cabs_parquet")

display(cabs_parquet.groupBy("PULocationID").agg(sum($"total_amount".cast(DoubleType)).as("sum")).orderBy($"sum".desc).limit(10))

// COMMAND ----------

// MAGIC %md Now, we'll write a Databricks Delta table using cabs data

// COMMAND ----------

cabs.write.format("delta").mode("overwrite").partitionBy("PULocationID").save("/tmp/cabs_delta")

// COMMAND ----------

// MAGIC %md OPTIMIZE the Databricks Delta table

// COMMAND ----------

display(spark.sql("DROP TABLE  IF EXISTS cabs"))

display(spark.sql("CREATE TABLE cabs USING DELTA LOCATION '/tmp/cabs_delta'"))
        
display(spark.sql("OPTIMIZE cabs ZORDER BY (payment_type)"))

// COMMAND ----------

// MAGIC %md Now rerun the query in `cmd 73` on Delta Table and observe the latency.

// COMMAND ----------

val cabs_delta = spark.read.format("delta").load("/tmp/cabs_delta")

display(cabs_delta.groupBy("PULocationID").agg(sum($"total_amount".cast(DoubleType)).as("sum")).orderBy($"sum".desc).limit(10))

// COMMAND ----------

// MAGIC %md The query over the Databricks Delta table runs much faster after OPTIMIZE is run. How much faster the query runs can depend on the configuration of the cluster you are running on, however should be **5-10X faster** compared to the standard table. 
// MAGIC 
// MAGIC In our scenario, the query on Parquet table took 22.8 seconds while the same query on Delta table took 4.98 seconds.

// COMMAND ----------

// MAGIC %md ##ACID TRANSACTIONAL GUARANTEE : CONCURRENCY CONTROL AND ISOLATION LEVELS##
// MAGIC 
// MAGIC Databricks Delta provides ACID transactional guarantees between reads and writes. This means that
// MAGIC  - Multiple writers, even if they are across multiple clusters, can simultaneously modify a dataset and see a consistent snapshot view of the table and there will be a serial order for these writes
// MAGIC  - Readers will continue to see the consistent snapshot view of the table that the spark job started with even when the table is modified during the job.
// MAGIC 
// MAGIC ###Optimistic Concurrency Control###
// MAGIC 
// MAGIC Delta uses [Optimistic Concurrency Control](https://en.wikipedia.org/wiki/Optimistic_concurrency_control) to provide transactional guarantees between writes. Under this mechanism, writers operate in three stages.
// MAGIC 
// MAGIC    - Read: A transactional write will start by reading (if needed) the latest available version of the table to identify which files need to be modified (that is, rewritten).
// MAGIC    - Write: Then it will stage all the changes by writing new data files.
// MAGIC    - Validate and commit: Finally, for committing the changes, in our optimistic concurrency protocol checks whether the proposed changes conflict with any other changes that may have been concurrently committed since the snapshot that was read. If there are no conflicts, then the all the staged changes will be committed as a new versioned snapshot, and the write operation succeeds. However, if there are conflicts, then the write operation will fail with a concurrent modification exception rather than corrupting the table as would happen with OSS Spark.