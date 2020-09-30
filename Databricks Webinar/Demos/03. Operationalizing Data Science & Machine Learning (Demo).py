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
# MAGIC 
# MAGIC <img src="https://mlflow.org/images/integration-logos/keras.png" alt="Keras logo" width="150">
# MAGIC 
# MAGIC Train the model and use MLflow to track its parameters, metrics, artifacts, and source code.
# MAGIC 
# MAGIC Define a power forecasting model in Keras, and create three different models with different configuratons and tuning parameters
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dmatrix/spark-saturday/raw/master/tutorials/mlflow/src/python/images/nn_linear_regression.png"
# MAGIC          alt="Keras NN Model as Logistic regression"  width="550">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md Iterate over three different set of tunning parameters—input_units, epochs, and batch_size— and track all its results

# COMMAND ----------

from hyperopt import fmin, hp, tpe, SparkTrials, STATUS_OK

def train_keras(params):
  keras_obj = KerasModel(X_train, input_units = int(params['input_units']), loss="mse", optimizer="adam", metrics=["mse"])
  loss = keras_obj.mlflow_run(X_train, y_train, epochs=100, batch_size=int(params['batch_size']), validation_split=.2, verbose=2)
  return {'status': STATUS_OK, 'loss': loss}

search_space = {
  'input_units':  hp.quniform('input_units', 100, 200, 50),
  'batch_size':   hp.quniform('batch_size', 64, 128, 32)
}
fmin(fn=train_keras, space=search_space, algo=tpe.suggest, max_evals=3, trials=SparkTrials(parallelism=3))

# COMMAND ----------

# MAGIC %md # Register the best model with the MLflow Model Registry
# MAGIC 
# MAGIC Now that a forecasting model has been trained and tracked with MLflow, the next step is to register it with the MLflow Model Registry. You can register and manage models using the MLflow UI.
# MAGIC 
# MAGIC <img src="https://databricks.com/wp-content/uploads/2019/10/model-registry-new.png"
# MAGIC          alt="MLflow model registry"  width="800">

# COMMAND ----------

# MAGIC %md ### Create a new Registered Model
# MAGIC 
# MAGIC First, navigate to the MLflow Runs Sidebar by clicking the `Runs` icon in the Databricks Notebook UI.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/runs_sidebar_icon.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="500">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC Next, locate the MLflow Run corresponding to the Keras model training session, and open it in the MLflow Run UI by clicking the `View Run Detail` icon.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/runs_sidebar_opened.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="300">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC In the MLflow UI, scroll down to the `Artifacts` section and click on the directory named `model`. Click on the `Register Model` button that appears.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/mlflow_ui_register_model.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="700">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC Then, select `Create New Model` from the drop-down menu, and input the following model name: `power-forecasting-model`. Finally, click `Register`. This registers a new model called `power-forecasting-model` and creates a new model version: `Version 1`.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/register_model_confirm.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="450">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC After a few moments, the MLflow UI displays a link to the new registered model. Follow this link to open the new model version in the MLflow Model Registry UI.

# COMMAND ----------

# MAGIC %md ### Explore the Model Registry UI
# MAGIC 
# MAGIC The Model Version page in the MLflow Model Registry UI provides information about `Version 1` of the registered forecasting model, including its author, creation time, and its current stage.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/registry_version_page.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="500">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC The Model Version page also provides a `Source Run` link, which opens the MLflow Run that was used to create the model in the MLflow Run UI. From the MLflow Run UI, you can access the `Source Notebook Link` to view a snapshot of the Databricks Notebook that was used to train the model.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/source_run_link.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="500">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/source_notebook_link.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="500">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC To navigate back to the MLflow Model Registry, click the `Models` icon in the Databricks Workspace Sidebar. The resulting MLflow Model Registry home page displays a list of all the registered models in your Databricks Workspace, including their versions and stages.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/registry_icon_sidebar.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="100">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC Select the `power-forecasting-model` link to open the Registered Model page, which displays all of the versions of the forecasting model.

# COMMAND ----------

# MAGIC %md ### Add model descriptions
# MAGIC 
# MAGIC You can add descriptions to Registered Models as well as Model Versions: 
# MAGIC * Model Version descriptions are useful for detailing the unique attributes of a particular Model Version (e.g., the methodology and algorithm used to develop the model). 
# MAGIC * Registered Model descriptions are useful for recording information that applies to multiple model versions (e.g., a general overview of the modeling problem and dataset).

# COMMAND ----------

# MAGIC %md Add a high-level description to the registered power forecasting model by clicking the `Edit Description` icon, entering the following description, and clicking `Save`:
# MAGIC 
# MAGIC ```
# MAGIC This model forecasts the power output of a wind farm based on weather data. The weather data consists of three features: wind speed, wind direction, and air temperature.
# MAGIC ```
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/model_description.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="800">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md Next, click the `Version 1` link from the Registered Model page to navigate back to the Model Version page. Then, add a model version description with information about the model architecture and machine learning framework; click the `Edit Description` icon, enter the following description, and click `Save`:
# MAGIC ```
# MAGIC This model version was built using Keras. It is a feed-forward neural network with one hidden layer.
# MAGIC ```
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/model_version_description.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="600">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md ### Perform a model stage transition
# MAGIC 
# MAGIC The MLflow Model Registry defines several model stages: `None`, `Staging`, `Production`, and `Archived`. Each stage has a unique meaning. For example, `Staging` is meant for model testing, while `Production` is for models that have completed the testing or review processes and have been deployed to applications. 
# MAGIC 
# MAGIC Users with appropriate permissions can transition models between stages. In private preview, any user can transition a model to any stage. In the near future, administrators in your organization will be able to control these permissions on a per-user and per-model basis.
# MAGIC 
# MAGIC If you have permission to transition a model to a particular stage, you can make the transition directly. If you do not have permission, you can request a stage transition from another user.

# COMMAND ----------

# MAGIC %md Click the `Stage` button to display the list of available model stages and your available stage transition options. Select `Transition to -> Production` and press `OK` in the stage transition confirmation window to transition the model to `Production`.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/stage_transition_prod.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="450">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/confirm_transition.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="450">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC After the model version is transitioned to `Production`, the current stage is displayed in the UI, and an entry is added to the activity log to reflect the transition.
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/stage_production.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="700">
# MAGIC   </td></tr>
# MAGIC </table>
# MAGIC 
# MAGIC <table>
# MAGIC   <tr><td>
# MAGIC     <img src="https://github.com/dbczumar/model-registry-demo-notebook/raw/master/ui_screenshots/activity_production.png"
# MAGIC          alt="MLflow Runs Sidebar Icon"  width="700">
# MAGIC   </td></tr>
# MAGIC </table>

# COMMAND ----------

# MAGIC %md # Integrate the model with the forecasting application
# MAGIC 
# MAGIC Now that you have trained and registered a power forecasting model with the MLflow Model Registry, the next step is to integrate it with an application. This application fetches a weather forecast for the wind farm over the next five days and uses the model to produce power forecasts. For example purposes, the application consists of a simple `forecast_power()` function (defined below) that is executed within this notebook. In practice, you may want to execute this function as a recurring batch inference job using the Databricks Jobs service.
# MAGIC 
# MAGIC The following **"Load versions of the registed model"** section demonstrates how to load model versions from the MLflow Model Registry for use in applications. Then, the **"Forecast power output with the production model"** section uses the `Production` model to forecast power output for the next five days.

# COMMAND ----------

# MAGIC %md ## Load versions of the registered model
# MAGIC 
# MAGIC The MLflow Models component defines functions for loading models from several machine learning frameworks. For example, `mlflow.keras.load_model()` is used to load Keras models that were saved in MLflow format, and `mlflow.sklearn.load_model()` is used to load scikit-learn models that were saved in MLflow format.
# MAGIC 
# MAGIC These functions can load models from the MLflow Model Registry.

# COMMAND ----------

# MAGIC %md You can load a model by specifying its name (e.g., `power-forecast-model`) and version number (e.g., `1`). The following cell uses the `mlflow.pyfunc.load_model()` API to load `Version 1` of the registered power forecasting model as a generic Python function.

# COMMAND ----------

import mlflow.pyfunc

model_name = "power-forecasting-model"
model_version_uri = f"models:/{model_name}/1"

print(f"Loading registered model version from URI: '{model_version_uri}'")
model_version_1 = mlflow.pyfunc.load_model(model_version_uri)

# COMMAND ----------

# MAGIC %md You can also load a specific model stage. The following cell loads the `Production` stage of the power forecasting model.

# COMMAND ----------

model_production_uri = f"models:/{model_name}/production"

print(f"Loading registered model version from URI: '{model_production_uri}'")
model_production = mlflow.pyfunc.load_model(model_production_uri)

# COMMAND ----------

# MAGIC %md ## Forecast power output with the production model
# MAGIC 
# MAGIC In this section, the production model is used to evaluate weather forecast data for the wind farm. The `PlotUtils.forecast_power()` class loads a version of the forecasting model from the specified URI and uses it to forecast power production over the next five days.

# COMMAND ----------

PlotUtils.forecast_power(model_production_uri, wind_farm_data)

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

model_version = 2 # If necessary, replace this with the version corresponding to the new scikit-learn model

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

client.update_model_version(
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

client.update_model_version(
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