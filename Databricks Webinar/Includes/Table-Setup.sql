-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px; height: 163px">
-- MAGIC </div>

-- COMMAND ----------

-- This assumes you've already uploaded the datasets to dbfs:/FileStore/tables

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS synpuf_csv;
USE synpuf_csv;

DROP TABLE IF EXISTS condition_occurrence;
DROP TABLE IF EXISTS drug_exposure;
DROP TABLE IF EXISTS person;

CREATE TABLE condition_occurrence
USING csv
OPTIONS (
  header=true,
  inferSchema=false,
  nullValue='null',
  timestampFormat="yyyy-MM-dd'T'hh:mm:ss.SSSZ")
LOCATION 'dbfs:/FileStore/tables/synpuf_condition_occurrence.csv';

CREATE TABLE drug_exposure
USING csv
OPTIONS (
  header=true,
  inferSchema=false,
  nullValue='null',
  timestampFormat="yyyy-MM-dd'T'hh:mm:ss.SSSZ")
LOCATION 'dbfs:/FileStore/tables/synpuf_drug_exposure.csv';

CREATE TABLE person
USING csv
OPTIONS (
  header=true,
  inferSchema=false,
  nullValue='null',
  timestampFormat="yyyy-MM-dd'T'hh:mm:ss.SSSZ")
LOCATION 'dbfs:/FileStore/tables/synpuf_person.csv';

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS synpuf_extra;
USE synpuf_extra;

DROP TABLE IF EXISTS condition_occurrence;
DROP TABLE IF EXISTS drug_exposure;
DROP TABLE IF EXISTS person;

CREATE TABLE condition_occurrence
USING csv
OPTIONS (
  header=true,
  inferSchema=true,
  nullValue='null',
  timestampFormat="yyyy-MM-dd'T'hh:mm:ss.SSSZ")
LOCATION 'dbfs:/FileStore/tables/synpuf_condition_occurrence.csv';

CREATE TABLE drug_exposure
USING csv
OPTIONS (
  header=true,
  inferSchema=true,
  nullValue='null',
  timestampFormat="yyyy-MM-dd'T'hh:mm:ss.SSSZ")
LOCATION 'dbfs:/FileStore/tables/synpuf_drug_exposure.csv';

CREATE TABLE person
(
  `person_id` INT,
  `gender_concept_id` INT,
  `year_of_birth` INT,
  `month_of_birth` INT,
  `day_of_birth` INT,
  `race_concept_id` INT,
  `ethnicity_concept_id` INT,
  `location_id` INT,
  `provider_id` INT,
  `care_site_id` INT,
  `person_source_value` STRING,
  `gender_source_value` INT,
  `race_source_value` INT,
  `ethnicity_source_value` INT)
USING csv
OPTIONS (
  header=true,
  inferSchema=false,
  nullValue='null',
  timestampFormat="yyyy-MM-dd'T'hh:mm:ss.SSSZ")
LOCATION 'dbfs:/FileStore/tables/synpuf_person.csv';

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS synpuf_delta;
USE synpuf_delta;

DROP TABLE IF EXISTS condition_occurrence;
DROP TABLE IF EXISTS drug_exposure;
DROP TABLE IF EXISTS person;

CREATE TABLE condition_occurrence
USING DELTA AS
SELECT * FROM synpuf_csv.condition_occurrence;

CREATE TABLE drug_exposure
USING DELTA AS
SELECT * FROM synpuf_csv.drug_exposure;

CREATE TABLE person
USING DELTA AS
SELECT * FROM synpuf_csv.person;

OPTIMIZE condition_occurrence ZORDER BY condition_source_value;
OPTIMIZE drug_exposure ZORDER BY drug_source_value;
OPTIMIZE person;

ANALYZE TABLE condition_occurrence COMPUTE STATISTICS;
ANALYZE TABLE drug_exposure COMPUTE STATISTICS;
ANALYZE TABLE person COMPUTE STATISTICS;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS synpuf;
USE synpuf;

DROP TABLE IF EXISTS condition_occurrence;
DROP TABLE IF EXISTS drug_exposure;
DROP TABLE IF EXISTS person;

CREATE TABLE condition_occurrence
USING DELTA AS
SELECT * FROM synpuf_csv.condition_occurrence;

CREATE TABLE drug_exposure
USING DELTA AS
SELECT * FROM synpuf_csv.drug_exposure;

CREATE TABLE person
USING DELTA AS
SELECT * FROM synpuf_csv.person;

OPTIMIZE condition_occurrence ZORDER BY condition_source_value;
OPTIMIZE drug_exposure ZORDER BY drug_source_value;
OPTIMIZE person;

ANALYZE TABLE condition_occurrence COMPUTE STATISTICS;
ANALYZE TABLE drug_exposure COMPUTE STATISTICS;
ANALYZE TABLE person COMPUTE STATISTICS;

-- COMMAND ----------

USE DEFAULT;


-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2019 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="http://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="http://help.databricks.com/">Support</a>