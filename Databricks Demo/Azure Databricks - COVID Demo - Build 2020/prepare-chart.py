# Databricks notebook source
# Standard Libraries
import io

# External Libraries
import requests
import numpy as np
import pandas as pd
import altair as alt

topo_usa = 'https://vega.github.io/vega-datasets/data/us-10m.json'
topo_wa = 'https://raw.githubusercontent.com/deldersveld/topojson/master/countries/us-states/WA-53-washington-counties.json'
topo_king = 'https://raw.githubusercontent.com/johan/world.geo.json/master/countries/USA/WA/King.geo.json'

# COMMAND ----------

# Convert latest date of data to pandas DataFrame
# pdf_usa = spark.sql("select * from df_usa").toPandas()
# pdf_usa['confirmed_per100K'] = pdf_usa['confirmed_per100K'].astype('int32')
# pdf_usa['deaths_per100K'] = pdf_usa['deaths_per100K'].astype('int32')

# COMMAND ----------

# Convert to Pandas
pdf_usa_conf = spark.sql("select * from df_usa_conf").toPandas()
pdf_usa_conf['day_num'] = pdf_usa_conf['day_num'].astype(str)
pdf_usa_conf['confirmed_per100K'] = pdf_usa_conf['confirmed_per100K'].astype('int64')
pdf_usa_conf = pdf_usa_conf.pivot_table(index='fips', columns='day_num', values='confirmed_per100K', fill_value=0).reset_index()

# COMMAND ----------

# Covnert to pandas
pdf_usa_deaths = spark.sql("select * from df_usa_deaths").toPandas()
pdf_usa_deaths['day_num'] = pdf_usa_deaths['day_num'].astype(str)
pdf_usa_deaths['deaths_per100K'] = pdf_usa_deaths['deaths_per100K'].astype('int64')
pdf_usa_deaths = pdf_usa_deaths.pivot_table(index=['lat', 'long_'], columns='day_num', values='deaths_per100K', fill_value=0).reset_index()

# COMMAND ----------

# Extract column names for slider
column_names = pdf_usa_conf.columns.tolist()

# Remove first element (`fips`)
column_names.pop(0)

# Convert to int
column_values = [None] * len(column_names)
for i in range(0, len(column_names)): column_values[i] = int(column_names[i]) 

# COMMAND ----------

# Disable max_rows to see more data
alt.data_transformers.disable_max_rows()

# Topographic information
us_states = alt.topo_feature(topo_usa, 'states')
us_counties = alt.topo_feature(topo_usa, 'counties')

# state borders
base_states = alt.Chart(us_states).mark_geoshape().encode(
  stroke=alt.value('lightgray'), fill=alt.value('white')
).properties(
  width=1200,
  height=960,
).project(
  type='albersUsa',
)

# Slider choices
min_day_num = column_values[0]
max_day_num = column_values[len(column_values)-1]
slider = alt.binding_range(min=min_day_num, max=max_day_num, step=1)
slider_selection = alt.selection_single(fields=['day_num'], bind=slider, name="day_num", init={'day_num':max_day_num})

# Confirmed cases by county
base_counties = alt.Chart(us_counties).mark_geoshape(
    stroke='black',
    strokeWidth=0.05
).project(
    type='albersUsa'
).transform_lookup(
    lookup='id',
    from_=alt.LookupData(pdf_usa_conf, 'fips', column_names)  
).transform_fold(
    column_names, as_=['day_num', 'confirmed_per100K']
).transform_calculate(
    day_num = 'parseInt(datum.day_num)',
    confirmed_per100K = 'isValid(datum.confirmed_per100K) ? datum.confirmed_per100K : -1'
).encode(
    color = alt.condition(
        'datum.confirmed_per100K > 0',      
        alt.Color('confirmed_per100K:Q', scale=alt.Scale(domain=(1, 7500), type='log')),
        alt.value('white')
      )  
).transform_filter(
    slider_selection
# ).add_selection(
#     slider_selection
)

# deaths by long, latitude
points = alt.Chart(
  pdf_usa_deaths
).mark_point(
  opacity=0.75, filled=True
).transform_fold(
  column_names, as_=['day_num', 'deaths_per100K']
).transform_calculate(
    day_num = 'parseInt(datum.day_num)',
    deaths_per100K = 'isValid(datum.deaths_per100K) ? datum.deaths_per100K : -1'  
).encode(
  longitude='long_:Q',
  latitude='lat:Q',
  size=alt.Size('deaths_per100K:Q', scale=alt.Scale(domain=(1, 1000), type='log'), title='deaths_per100K'),
  color=alt.value('#BD595D'),
  stroke=alt.value('brown'),
).properties(
  # update figure title
  title=f'COVID-19 Confirmed Cases and Deaths by County (per 100K) Between 3/22 to 4/14 (2020)'
).add_selection(
    slider_selection
).transform_filter(
    slider_selection
)

# confirmed cases (base_counties) and deaths (points)
(base_states + base_counties + points) 