# Databricks notebook source
# MAGIC %md
# MAGIC # Operationalizing Data Science & Machine Learning: Demo
# MAGIC 
# MAGIC ## Big data challenge #3: Operationalizing machine learning is hard
# MAGIC 
# MAGIC <img src="https://pages.databricks.com/rs/094-YMS-629/images/3 - Unify data and ML across the full lifecycle.png" width="1000">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The zoo of ML frameworks
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/02/Zoo-of-Analytics-Frameworks.png" alt="Zoo of analytics frameworks"  width="600">

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## The machine learning lifecycle
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2020/03/Machine-Learning-Lifecycle.jpg" alt="Machine learning lifecycle diagram" width="600">

# COMMAND ----------

# MAGIC %md **Note:** To properly run this notebook, you will need access to the MLflow Model Registry. Contact your Databricks representative to be part of the Model Registry public preview.

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC ### Tracking Experiments with MLflow
# MAGIC 
# MAGIC Over the course of the machine learning lifecycle, data scientists test many different models from various libraries with different hyperparemeters.  Tracking these various results poses an organizational challenge.  In brief, storing experiements, results, models, supplementary artifacts, and code creates significant challenges in the machine learning lifecycle.
# MAGIC 
# MAGIC MLflow Tracking is a logging API specific for machine learning and agnostic to libraries and environments that do the training.  It is organized around the concept of **runs**, which are executions of data science code.  Runs are aggregated into **experiments** where many runs can be a part of a given experiment and an MLflow server can host many experiments.
# MAGIC 
# MAGIC Each run can record the following information:
# MAGIC 
# MAGIC - **Parameters:** Key-value pairs of input parameters such as the number of trees in a random forest model
# MAGIC - **Metrics:** Evaluation metrics such as RMSE or Area Under the ROC Curve
# MAGIC - **Artifacts:** Arbitrary output files in any format.  This can include images, pickled models, and data files
# MAGIC - **Source:** The code that originally ran the experiement
# MAGIC 
# MAGIC MLflow tracking also serves as a **model registry** so tracked models can easily be stored and, as necessary, deployed into production.
# MAGIC 
# MAGIC Experiments can be tracked using libraries in Python, R, and Java as well as by using the CLI and REST calls.  This course will use Python, though the majority of MLflow funcionality is also exposed in these other APIs.
# MAGIC 
# MAGIC <div><img src="https://files.training.databricks.com/images/eLearning/ML-Part-4/mlflow-tracking.png" style="height: 300px; margin: 20px"/></div>

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ## Wind Power Forecasting Demo
# MAGIC 
# MAGIC In this notebook, you will use the MLflow Model Registry to build a machine learning application that forecasts the daily power output of a [wind farm](https://en.wikipedia.org/wiki/Wind_farm). Wind farm power output depends on weather conditions: generally, more energy is produced at higher wind speeds. Accordingly, the machine learning models used in the notebook predict power output based on weather forecasts with three features: `wind direction`, `wind speed`, and `air temperature`.
# MAGIC 
# MAGIC <img src="https://databricks.com/wind-turbines" alt="Wind turbines" width="600"/>
# MAGIC 
# MAGIC ### Setup instructions
# MAGIC 
# MAGIC The following setup procedure is required in order to run the example notebook:
# MAGIC 
# MAGIC 1. Start a Databricks cluster with the Databricks Machine Learning Runtime (MLR) version 6.0 or later. Python 3 is recommended.
# MAGIC 2. Install MLflow 1.7.0+ and attach it to your cluster, if using DBR MLR < 6.5.
# MAGIC 3. Attach this example notebook to your cluster.

# COMMAND ----------

# MAGIC %md ## Load the dataset
# MAGIC 
# MAGIC The following cells load a dataset containing weather data and power output information for a wind farm in the United States. The dataset contains `wind direction`, `wind speed`, and `air temperature` features sampled every six hours (once at `00:00`, once at `08:00`, and once at `16:00`), as well as daily aggregate power output (`power`), over several years.

# COMMAND ----------

# MAGIC %md Run the notebook to bring defined classes for models and utilities into the scope of this notebook.

# COMMAND ----------

# MAGIC %run ../Includes/classes_init

# COMMAND ----------

# MAGIC %md Load the data, and display a sample of it for reference.

# COMMAND ----------

csv_path = "https://github.com/dmatrix/spark-saturday/raw/master/tutorials/mlflow/src/python/labs/data/windfarm_data.csv"
wind_farm_data = Utils.load_data(csv_path, index_col=0)

wind_farm_data.head()

# COMMAND ----------

# MAGIC %md ### Get Training and Validation Data

# COMMAND ----------

X_train, y_train = Utils.get_training_data(wind_farm_data)
val_x, val_y = Utils.get_validation_data(wind_farm_data)

# COMMAND ----------

# MAGIC %md # Train a power forecasting model and track it with MLflow
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2018/06/mlflow.png"
# MAGIC          alt="MLflow tracking, projects, and models"  width="600">
# MAGIC 
# MAGIC The following cells train a neural network in Keras to predict power output based on the weather features in the dataset. MLflow is used to track the model's hyperparameters, performance metrics, source code, and artifacts.

# COMMAND ----------

# MAGIC %md # Register the best model with the MLflow Model Registry
# MAGIC 
# MAGIC Now that a forecasting model has been trained and tracked with MLflow, the next step is to register it with the MLflow Model Registry. You can register and manage models using the MLflow UI.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/10/model-registry-new.png"
# MAGIC          alt="MLflow model registry"  width="800">

# COMMAND ----------

# MAGIC %md ## Part 2: Use a Scikit-learn Random Forest Regressor to create the second model. 
# MAGIC 
# MAGIC <img src="https://mlflow.org/images/integration-logos/scikit-learn.png" alt="scikit-learn logo" width="150">
# MAGIC 
# MAGIC This model, in real development environment, could be developed by another developer and registered in the model registry. 
# MAGIC 
# MAGIC Use MLflow to compare and then elect the best model between Keras and Random Forest for production.

# COMMAND ----------

# MAGIC %md # Create and deploy a new model version
# MAGIC 
# MAGIC The MLflow Model Registry enables you to create multiple model versions corresponding to a single registered model. By performing stage transitions, you can seamlessly integrate new model versions into your staging or production environments. Model versions can be trained in different machine learning frameworks (e.g., `scikit-learn` and `Keras`); MLflow's `python_function` provides a consistent inference API across machine learning frameworks, ensuring that the same application code continues to work when a new model version is introduced.
# MAGIC 
# MAGIC The following sections create a new version of the power forecasting model using scikit-learn, perform model testing in `Staging`, and update the production application by transitioning the new model version to `Production`.

# COMMAND ----------

# MAGIC %md ## Create a new model version
# MAGIC 
# MAGIC Classical machine learning techniques are also effective for power forecasting. The following cell trains a random forest model using scikit-learn and registers it with the MLflow Model Registry via the `mlflow.sklearn.log_model()` function.
# MAGIC 
# MAGIC As above, we will try few different parameters and choose the best model.

# COMMAND ----------

model_name = "power-forecasting-model"

params_list = [
        {"n_estimators": 100},
        {"n_estimators": 200},
        {"n_estimators": 300}]
#iterate over few different tuning parameters
for params in params_list:
  rfr = RFRModel.new_instance(params)
  print(f"Using parameters={params}")
  runID = rfr.mlflow_run(X_train, y_train, val_x, val_y, model_name)
  print(f"MLflow run_id={runID} completed with MSE={rfr.mse} and RMSE={rfr.rsme}")

# COMMAND ----------

# MAGIC %md When a new model version is created with the MLflow Model Registry's Python APIs, the name and version information is printed for future reference. You can also navigate to the MLflow Model Registry UI to view the new model version. 
# MAGIC 
# MAGIC Define the new model version as a variable in the cell below.

# COMMAND ----------

model_version = 4 # If necessary, replace this with the version corresponding to the new scikit-learn model

# COMMAND ----------

# MAGIC %md Wait for the new model version to become ready.

# COMMAND ----------

Utils.wait_until_ready(model_name, model_version)

# COMMAND ----------

# MAGIC %md ## Add a description to the new model version

# COMMAND ----------

from mlflow.tracking.client import MlflowClient

client = MlflowClient()
client.update_model_version(
  name=model_name,
  version=model_version,
  description="This is the best model version of a random forest that was trained in scikit-learn."
)

# COMMAND ----------

# MAGIC %md ## Test the new model version in `Staging`
# MAGIC 
# MAGIC Before deploying a model to a production application, it is often best practice to test it in a staging environment. The following cells transition the new model version to `Staging` and evaluate its performance.

# COMMAND ----------

client.transition_model_version_stage(
  name=model_name,
  version=model_version,
  stage="Staging",
)

# COMMAND ----------

# MAGIC %md Evaluate the new model's forecasting performance in `Staging`

# COMMAND ----------

model_staging_uri = f"models:/{model_name}/staging"
PlotUtils.forecast_power(model_staging_uri, wind_farm_data)

# COMMAND ----------

# MAGIC %md ## Deploy the new model version to `Production`
# MAGIC 
# MAGIC After verifying that the new model version performs well in staging, the following cells transition the model to `Production` and use the exact same application code from the **"Forecast power output with the production model"** section to produce a power forecast. 
# MAGIC 
# MAGIC **The MLflow Model Model Registry automatically uses the latest production version of the specified model, allowing you to update your production models without changing any application code**.

# COMMAND ----------

client.transition_model_version_stage(
  name=model_name,
  version=model_version,
  stage="Production"
)

# COMMAND ----------

model_production_uri = f"models:/{model_name}/production"
PlotUtils.forecast_power(model_production_uri, wind_farm_data)

# COMMAND ----------

# MAGIC %md # Delete models
# MAGIC 
# MAGIC When a model version is no longer being used, you can archive it or delete it. You can also delete an entire registered model; this removes all of its associated model versions.

# COMMAND ----------

# MAGIC %md ### Workflow 1: Delete `Version 1` in the MLflow UI
# MAGIC 
# MAGIC To delete `Version 1` of the power forecasting model, open its corresponding Model Version page in the MLflow Model Registry UI. Then, select the drop-down arrow next to the version identifier and click `Delete`.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/delete_version.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="700">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md ### Workflow 2: Delete `Version 1` using the MLflow API
# MAGIC 
# MAGIC The following cell permanently deletes `Version 1` of the power forecasting model. If you want to delete this model version, uncomment and execute the cell.

# COMMAND ----------

# client.delete_model_version(name=model_name, version=1)

# COMMAND ----------

# MAGIC %md ## Delete the power forecasting model
# MAGIC 
# MAGIC If you want to delete an entire registered model, including all of its model versions, you can use the `MlflowClient.delete_registered_model()` to do so. This action cannot be undone.
# MAGIC 
# MAGIC **WARNING: The following cell permanently deletes the power forecasting model, including all of its versions.** If you want to delete the model, uncomment and execute the cell. 

# COMMAND ----------

#### REMOVE THIS CELL BEFORE DISTRIBUTING ####
# client.delete_registered_model(name=model_name)

# COMMAND ----------

# DBTITLE 1,Citations
# MAGIC %md *This notebook uses altered data from the [National WIND Toolkit dataset](https://www.nrel.gov/grid/wind-toolkit.html) provided by NREL, which is publicly available and cited as follows:*
# MAGIC - <sup><sub>*Draxl, C., B.M. Hodge, A. Clifton, and J. McCaa. 2015. Overview and Meteorological Validation of the Wind Integration National Dataset Toolkit (Technical Report, NREL/TP-5000-61740). Golden, CO: National Renewable Energy Laboratory.*
# MAGIC - *Draxl, C., B.M. Hodge, A. Clifton, and J. McCaa. 2015. "The Wind Integration National Dataset (WIND) Toolkit." Applied Energy 151: 355366.*
# MAGIC - *Lieberman-Cribbin, W., C. Draxl, and A. Clifton. 2014. Guide to Using the WIND Toolkit Validation Code (Technical Report, NREL/TP-5000-62595). Golden, CO: National Renewable Energy Laboratory.*
# MAGIC - *King, J., A. Clifton, and B.M. Hodge. 2014. Validation of Power Output for the WIND Toolkit (Technical Report, NREL/TP-5D00-61714). Golden, CO: National Renewable Energy Laboratory.* </sub></sup>