# Databricks notebook source
# MAGIC %md ## MLflow Quick Start: Tracking
# MAGIC This is a notebook based on the MLflow quick start example.  This quick start:
# MAGIC * Starts an MLflow run
# MAGIC * Logs parameters, metrics, and a file to the run

# COMMAND ----------

# MAGIC %md ### Set up and attach notebook to cluster

# COMMAND ----------

# MAGIC %md 
# MAGIC 1. Use or create a cluster with:
# MAGIC   * **Databricks Runtime Version:** Databricks Runtime 5.0 or above 
# MAGIC   * **Python Version:** Python 3
# MAGIC 1. Install MLflow library.
# MAGIC    1. Create library with Source **PyPI** and enter `mlflow`.
# MAGIC 1. Attach this notebook to the cluster.

# COMMAND ----------

# MAGIC %pip install azureml-sdk[databricks]

# COMMAND ----------

# MAGIC %pip install azureml-mlflow

# COMMAND ----------

import azureml
from azureml.core import Workspace

workspace_name = "mlops-demo"
workspace_location="westeurope"
resource_group = "mlops-rg"
subscription_id = "6ee947fa-0d77-4915-bf68-4a83a8bec2a4"

workspace = Workspace(subscription_id, resource_group, workspace_name)

# COMMAND ----------

workspace.get_details()

# COMMAND ----------

# MAGIC %md ### Import MLflow

# COMMAND ----------

import mlflow

# COMMAND ----------

# MAGIC %md ### Use the MLflow Tracking API
# MAGIC 
# MAGIC Use the [MLflow Tracking API](https://www.mlflow.org/docs/latest/python_api/index.html) to start a run and log parameters, metrics, and artifacts (files) from your data science code. 

# COMMAND ----------

# Start an MLflow run

with mlflow.start_run():
  # Log a parameter (key-value pair)
  mlflow.log_param("param2", 3)

  # Log a metric; metrics can be updated throughout the run
  mlflow.log_metric("foo", 2, step=1)
  mlflow.log_metric("foo", 4, step=2)
  mlflow.log_metric("foo", 6, step=3)


  # Log an artifact (output file)
  with open("output.txt", "w") as f:
      f.write("Hello world!")
  mlflow.log_artifact("output.txt")

# COMMAND ----------

# MAGIC %md ## Switch to Azure Machine Learning backend

# COMMAND ----------

workspace.get_mlflow_tracking_uri()

# COMMAND ----------

mlflow.get_tracking_uri()

# COMMAND ----------

# change to AML
mlflow.set_tracking_uri(workspace.get_mlflow_tracking_uri())
mlflow.get_tracking_uri()

# COMMAND ----------

# # revert changes back to databricks
# mlflow.set_tracking_uri('databricks')
# mlflow.get_tracking_uri()

# COMMAND ----------

experiment_name = 'experiment_with_mlflow_from_databricks'
mlflow.set_experiment(experiment_name)

# COMMAND ----------

# Start an MLflow run

with mlflow.start_run():
  # Log a parameter (key-value pair)
  mlflow.log_param("param2", 3)

  # Log a metric; metrics can be updated throughout the run
  mlflow.log_metric("foo", 2, step=1)
  mlflow.log_metric("foo", 4, step=2)
  mlflow.log_metric("foo", 6, step=3)


  # Log an artifact (output file)
  with open("output.txt", "w") as f:
      f.write("Hello world!")
  mlflow.log_artifact("output.txt")

# COMMAND ----------

