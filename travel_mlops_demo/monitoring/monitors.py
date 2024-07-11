
# Databricks notebook source

# COMMAND ----------
%pip install "https://ml-team-public-read.s3.amazonaws.com/wheels/data-monitoring/a4050ef7-b183-47a1-a145-e614628e3146/databricks_lakehouse_monitoring-0.4.14-py3-none-any.whl"

# COMMAND ----------
from databricks import lakehouse_monitoring as lm


table_names=["asong_demo.travel_raw.nyctaxi_rides"
             ,"asong_dev.travel_mlops_demo.feature_store_inference_input"
             ,"asong_dev.travel_mlops_demo.predictions"
             ,"asong_dev.travel_mlops_demo.trip_dropoff_features"
             , "asong_dev.travel_mlops_demo.trip_pickup_features"]


lm.create_monitor(
    table_name=f"{catalog}.{schema}.{table_name}",
    profile_type=lm.TimeSeries(
        timestamp_col="ts",
        granularities=["30 minutes"]
    ),
    output_schema_name=f"{catalog}.{schema}"
)