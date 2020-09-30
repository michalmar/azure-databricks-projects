# Databricks notebook source
# MAGIC %md ### Classes and Utility functions for the Model Registry Demo Notebook

# COMMAND ----------

import pandas as pd
import time
from mlflow.tracking.client import MlflowClient
from mlflow.entities.model_registry.model_version_status import ModelVersionStatus

# COMMAND ----------

import warnings
warnings.filterwarnings("ignore")

# COMMAND ----------

class Utils:
  @staticmethod
  def load_data(path, index_col=0):
    df = pd.read_csv(path,index_col=0)
    return df
  
  @staticmethod
  def get_training_data(df):
    training_data = pd.DataFrame(df["2014-01-01":"2018-01-01"])
    X = training_data.drop(columns="power")
    y = training_data["power"]
    return X, y

  @staticmethod
  def get_validation_data(df):
    validation_data = pd.DataFrame(df["2018-01-01":"2019-01-01"])
    X = validation_data.drop(columns="power")
    y = validation_data["power"]
    return X, y

  @staticmethod
  def get_weather_and_forecast(df):
    format_date = lambda pd_date : pd_date.date().strftime("%Y-%m-%d")
    today = pd.Timestamp('today').normalize()
    week_ago = today - pd.Timedelta(days=5)
    week_later = today + pd.Timedelta(days=5)

    past_power_output = pd.DataFrame(df)[format_date(week_ago):format_date(today)]
    weather_and_forecast = pd.DataFrame(df)[format_date(week_ago):format_date(week_later)]
    if len(weather_and_forecast) < 10:
      past_power_output = pd.DataFrame(df).iloc[-10:-5]
      weather_and_forecast = pd.DataFrame(df).iloc[-10:]

    return weather_and_forecast.drop(columns="power"), past_power_output["power"]
  
  @staticmethod
  def wait_until_ready(model_name, model_version):
    client = MlflowClient()
    for _ in range(10):
      model_version_details = client.get_model_version_details(
        name=model_name,
        version=model_version,
      )
      status = ModelVersionStatus.from_string(model_version_details.status)
      print("Model status: %s" % ModelVersionStatus.to_string(status))
      if status == ModelVersionStatus.READY:
        break
      time.sleep(1)

# COMMAND ----------

displayHTML("""
<div> Declared Utils class with utility methods:</div> 
  <li> Declared <b style="color:green">load_data(path, index_col=0)</b> returns Pandas DataFrame for diagnostics</li>
  <li> Declared <b style="color:green">get_training_data(df)</b> returns X, y Pandas dataframe</li>
  <li> Declared <b style="color:green">get_validation_data(df)</b> returns val_x, val_y Pandas dataframe</li>
  <li> Declared <b style="color:green">get_weather_and_forecast(df) returns Pandas Dataframe with dropped "power" columns</b></li>
  <li> Declared <b style="color:green">wait_until_ready(model_name, model_version)</b></li>
   <br/>
""")

# COMMAND ----------

import pandas as pd
import matplotlib.dates as mdates
from matplotlib import pyplot as plt

# COMMAND ----------

class PlotUtils:
    @staticmethod
    def plot(model_uri, power_predictions, past_power_output):
      index = power_predictions.index
      fig = plt.figure(figsize=(11, 7))
      ax = fig.add_subplot(111)
      ax.set_xlabel("Date", size=20, labelpad=20)
      ax.set_ylabel("Power\noutput\n(MW)", size=20, labelpad=60, rotation=0)
      ax.tick_params(axis='both', which='major', labelsize=17)
      ax.xaxis.set_major_formatter(mdates.DateFormatter('%m/%d'))
      ax.plot(index[:len(past_power_output)], past_power_output, label="True", color="red", alpha=0.5, linewidth=4)
      ax.plot(index, power_predictions, "--", label="Predicted by {}".format(model_uri), color="blue", linewidth=3)
      ax.set_ylim(ymin=0, ymax=max(3500, int(max(power_predictions.values) * 1.3)))
      ax.legend(fontsize=14)
      plt.title("Wind farm power output and projections", size=24, pad=20)
      plt.tight_layout()
      display(plt.show())
      
    @staticmethod
    def forecast_power(model_uri, wind_farm_data):
      print("Loading registered model version from URI: '{model_uri}'".format(model_uri=model_uri))
      model = mlflow.pyfunc.load_model(model_uri)
      weather_data, past_power_output = Utils.get_weather_and_forecast(wind_farm_data)
      power_predictions = pd.DataFrame(model.predict(weather_data))
      power_predictions.index = pd.to_datetime(weather_data.index)
      PlotUtils.plot(model_uri, power_predictions, past_power_output)

# COMMAND ----------

displayHTML("""
<div> Declared PlotUtils class with utility methods:</div> 
  <li> Declared <b style="color:green">plot(model_uri, power_predictions, past_power_output)</b> Plots a graph </li>
  <li> Declared <b style="color:green">forecast_power(model_uri, df)</b> Plots a graph</b></li>
   <br/>
""")

# COMMAND ----------

# import keras
# from keras.models import Sequential
# from keras.layers import Dense

import tensorflow.keras as keras
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

import mlflow
import mlflow.keras

print("Using mlflow version {}".format(mlflow.__version__))

# COMMAND ----------

class KerasModel:
  def __init__(self, X_train, input_units = 100, activation="relu", **kwargs):
    self._model = Sequential()
    self._model.add(Dense(input_units, input_shape=(X_train.shape[-1],), activation=activation, name="hidden_layer"))
    self._model.add(Dense(1))
    self._model.compile(**kwargs)
  
  def mlflow_run(self, X_train, y_train, run_name="Keras NN: Power Forecasting Model", **kwargs):
    mlflow.keras.autolog()
    hist = self._model.fit(X_train, y_train, **kwargs)
    mse = hist.history['loss'][-1]
    #mlflow.log_metric("mse", mse)
    return mse

# COMMAND ----------

displayHTML("""
<div> Declared KerasModel class with public methods:</div> 
  <li> Declared <b style="color:green"> mlflow_run(model, X_train, y_train, **kwargs)</b> returns MLflow run_id </li>
  <br/>
""")

# COMMAND ----------

import mlflow.sklearn
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error

# COMMAND ----------

class RFRModel():
  def __init__(self, params={}):
    self.rf = RandomForestRegressor(**params)
    self.params = params
    self._mse = None
    self._rsme = None
    
  @classmethod
  def new_instance(cls, params={}):
    return cls(params)
  
  def model(self):
    return self.rf
  
  @property
  def mse(self):
    return self._mse
  
  @mse.setter
  def mse(self, value):
    self._mse = value
  
  @property
  def rsme(self):
    return self._rsme
  
  @rsme.setter
  def rsme(self, value):
    self._rsme = value
  
  def mlflow_run(self, X_train, y_train, val_x, val_y, model_name, run_name="Random Forest Regressor: Power Forecasting Model"):
    with mlflow.start_run(run_name=run_name) as run:
      mlflow.log_params(self.params)
      self.rf.fit(X_train, y_train)
      self._mse = mean_squared_error(self.rf.predict(val_x), val_y)
      self._rsme = np.sqrt(self._mse)
      print("Validation MSE: %d" % self._mse)
      print("Validation RMSE: %d" % self._rsme)
      mlflow.log_metric("mse", self._mse)
      mlflow.log_metric("rmse", self._rsme)
      # Specify the `registered_model_name` parameter of the `mlflow.sklearn.log_model()`
      # function to register the model with the MLflow Model Registry. This automatically
      # creates a new model version
      mlflow.sklearn.log_model(
        sk_model= self.model(),
        artifact_path="sklearn-model",
        registered_model_name=model_name,
      )
      run_id = run.info.run_id
    return run_id

# COMMAND ----------

displayHTML("""
<div> Declared RFRModel class with public methods:</div> 
  <li> Declared <b style="color:green">  mlflow_run(X_train, y_train, val_x, val_y, model_name, run_name="Random Forest Regressor: Power Forecasting Model)</b> returns MLflow run_id </li>
   <li> Declared <b style="color:green">mse()</b> returns models 'mse' accuracy</li>
   <li> Declared <b style="color:green">rmse()</b> returns models 'rmse' accuracy</li>
  <br/>
<div> All done!</div>
""")

# COMMAND ----------

