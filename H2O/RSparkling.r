# Databricks notebook source
# Install Sparklyr
install.packages("sparklyr")


# install H2O
install.packages("RCurl")
# install.packages("h2o", type="source", repos="https://h2o-release.s3.amazonaws.com/h2o/rel-yates/2/R")
install.packages("h2o", type = "source", repos = "http://h2o-release.s3.amazonaws.com/h2o/rel-zermelo/1/R")

# Install RSparkling 3.32.0.1-2-3.0
install.packages("rsparkling", type = "source", repos = "http://h2o-release.s3.amazonaws.com/sparkling-water/spark-3.0/3.32.0.1-2-3.0/R")


# COMMAND ----------

library(rsparkling)
library(sparklyr)

sc <- spark_connect(method="databricks")

# h2o_context(sc)
H2OContext(sc)