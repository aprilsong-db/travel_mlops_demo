# Databricks notebook source
##################################################################################
# Prep Batch Inference Input Notebook
#
# This notebook is an example of applying a model for batch inference against an input delta table,
# It is configured and can be executed as the batch_inference_job in the batch_inference_job workflow defined under
# ``travel_mlops_demo/resources/batch-inference-workflow-resource.yml``
#
# Parameters:
#
#  * env (optional)  - String name of the current environment (dev, staging, or prod). Defaults to "dev"
#  * output_table_name (required) - Delta table name where the predictions will be written to.
#                                   Note that this will create a new version of the Delta table if
#                                   the table already exists
#  * model_name (required) - The name of the model to be used in batch inference.
##################################################################################
from pyspark.sql.functions import to_timestamp, lit
from pyspark.sql.types import IntegerType
import math
from datetime import timedelta, timezone


# List of input args needed to run the notebook as a job.
# Provide them via DB widgets or notebook arguments.
#
# Name of the current environment
dbutils.widgets.dropdown("env", "dev", ["dev", "staging", "prod"], "Environment Name")
# Delta table containing the new data to perform batch prediction
dbutils.widgets.text(
    "input_table_name",
    "/databricks-datasets/nyctaxi-with-zipcodes/subsampled",
    label="Input Table Name",
)
# Delta table to store the output predictions.
dbutils.widgets.text("output_table_name", "", label="Output Table Name")

env = dbutils.widgets.get("env")
input_table_name = dbutils.widgets.get("input_table_name")
output_table_name = dbutils.widgets.get("output_table_name")


def rounded_unix_timestamp(dt, num_minutes=15):
    """
    Ceilings datetime dt to interval num_minutes, then returns the unix timestamp.
    """
    nsecs = dt.minute * 60 + dt.second + dt.microsecond * 1e-6
    delta = math.ceil(nsecs / (60 * num_minutes)) * (60 * num_minutes) - nsecs
    return int((dt + timedelta(seconds=delta)).replace(tzinfo=timezone.utc).timestamp())


rounded_unix_timestamp_udf = udf(rounded_unix_timestamp, IntegerType())

df = spark.table(input_table_name)
df.withColumn(
    "rounded_pickup_datetime",
    to_timestamp(rounded_unix_timestamp_udf(df["tpep_pickup_datetime"], lit(15))),
).withColumn(
    "rounded_dropoff_datetime",
    to_timestamp(rounded_unix_timestamp_udf(df["tpep_dropoff_datetime"], lit(30))),
).drop(
    "tpep_pickup_datetime"
).drop(
    "tpep_dropoff_datetime"
).drop(
    "fare_amount"
).write.mode(
    "overwrite"
).saveAsTable(
    name=output_table_name
)
