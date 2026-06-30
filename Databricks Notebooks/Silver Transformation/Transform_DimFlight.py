# Databricks notebook source
# MAGIC %md
# MAGIC ## ✈️ DimFlight - Bronze to Silver Ingestion Pipeline
# MAGIC
# MAGIC #### **Overview**
# MAGIC This notebook reads raw flight dimension data from the **Bronze** storage container, performs data cleaning, standardizes values, enriches the records with region mappings, and saves the cleaned dataset as a Delta table in the **Silver** container.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Pipeline Metadata**
# MAGIC | Attribute | Detail |
# MAGIC | :--- | :--- |
# MAGIC | **Source Path** | `abfss://bronze@adfkeshavstorage.dfs.core.windows.net/onprem/DimFlight.csv` |
# MAGIC | **Target Path** | `abfss://silver@adfkeshavstorage.dfs.core.windows.net/DimFlight` |
# MAGIC | **Format** | CSV (Bronze) $\rightarrow$ Delta (Silver) |
# MAGIC | **Target Type** | Slowly Changing Dimension (SCD) Type 1 / Lookup Table |
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Define the path for both the containers
bronze_path  = "abfss://bronze@adfkeshavstorage.dfs.core.windows.net/onprem/DimFlight.csv"
silver_path  = "abfss://silver@adfkeshavstorage.dfs.core.windows.net/DimFlight"

# COMMAND ----------

# 1. Define Schema
flight_schema = StructType([
    StructField("flight_id", IntegerType(), True),
    StructField("flight_number", StringType(), True),
    StructField("departure_time", StringType(), True),
    StructField("arrival_time", StringType(), True)
])
# read the data
bronze_flight_df = spark.read.option("header", "true").schema(flight_schema).csv(bronze_path)

# COMMAND ----------

# 3. Apply Transformations
silver_flight_df = bronze_flight_df \
    .filter(F.col("flight_id").isNotNull()) \
    .dropDuplicates(["flight_id"]) \
    .withColumn("flight_number", F.upper(F.trim(F.col("flight_number")))) \
    .withColumn("departure_time", F.trim(F.col("departure_time"))) \
    .withColumn("arrival_time", F.trim(F.col("arrival_time"))) \
    .withColumn("airline_code", F.substring(F.col("flight_number"), 1, 2)) \
    .withColumn("dep_hour", F.split(F.col("departure_time"), ":").getItem(0).cast("int")) \
    .withColumn("dep_min", F.split(F.col("departure_time"), ":").getItem(1).cast("int")) \
    .withColumn("arr_hour", F.split(F.col("arrival_time"), ":").getItem(0).cast("int")) \
    .withColumn("arr_min", F.split(F.col("arrival_time"), ":").getItem(1).cast("int")) \
    .withColumn("dep_total_mins", (F.col("dep_hour") * 60) + F.col("dep_min")) \
    .withColumn("arr_total_mins", (F.col("arr_hour") * 60) + F.col("arr_min")) \
    .withColumn("duration_minutes", 
        F.when(F.col("arr_total_mins") >= F.col("dep_total_mins"), 
               F.col("arr_total_mins") - F.col("dep_total_mins"))
         .otherwise((F.col("arr_total_mins") + 1440) - F.col("dep_total_mins"))
    ) \
    .withColumn("flight_duration", 
        F.concat(
            F.floor(F.col("duration_minutes") / 60).cast("string"), F.lit("h "),
            (F.col("duration_minutes") % 60).cast("string"), F.lit("m")
        )
    ) \
    .withColumn("departure_period",
        F.when((F.col("dep_hour") >= 6) & (F.col("dep_hour") < 12), "Morning")
         .when((F.col("dep_hour") >= 12) & (F.col("dep_hour") < 17), "Afternoon")
         .when((F.col("dep_hour") >= 17) & (F.col("dep_hour") < 22), "Evening")
         .otherwise("Night")
    ) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("flight_sk", F.sha2(F.col("flight_id").cast("string"), 256)) \
    .drop("dep_hour", "dep_min", "arr_hour", "arr_min", "dep_total_mins", "arr_total_mins")

# COMMAND ----------

display(silver_flight_df)

# COMMAND ----------

silver_flight_df.write.format("delta").mode("overwrite").save(silver_path)