# Databricks notebook source
# MAGIC %md
# MAGIC ## ✈️ DimPassenger - Bronze to Silver Ingestion Pipeline
# MAGIC
# MAGIC #### **Overview**
# MAGIC This notebook reads raw passenger dimension data from the **Bronze** storage container, performs data cleaning, standardizes values, enriches the records with region mappings, and saves the cleaned dataset as a Delta table in the **Silver** container.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Pipeline Metadata**
# MAGIC | Attribute | Detail |
# MAGIC | :--- | :--- |
# MAGIC | **Source Path** | `abfss://bronze@adfkeshavstorage.dfs.core.windows.net/onprem/DimPassenger.csv` |
# MAGIC | **Target Path** | `abfss://silver@adfkeshavstorage.dfs.core.windows.net/DimPassenger` |
# MAGIC | **Format** | CSV (Bronze) $\rightarrow$ Delta (Silver) |
# MAGIC | **Target Type** | Slowly Changing Dimension (SCD) Type 1 / Lookup Table |
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Define the path for both the containers
bronze_path  = "abfss://bronze@adfkeshavstorage.dfs.core.windows.net/onprem/DimPassenger.csv"
silver_path  = "abfss://silver@adfkeshavstorage.dfs.core.windows.net/DimPassenger"

# COMMAND ----------

# 1. Define Schema
passenger_schema = StructType([
    StructField("passenger_id", IntegerType(), True),
    StructField("full_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True)
])
# 2. Read Bronze Data
bronze_passenger_df = spark.read.option("header", "true").schema(passenger_schema).csv(bronze_path)

# COMMAND ----------

# Country ISO mapping (similar to DimAirline)
country_iso_map = F.create_map([
    F.lit("USA"), F.lit("US"),
    F.lit("Canada"), F.lit("CA"),
    F.lit("India"), F.lit("IN"),
    F.lit("China"), F.lit("CN"),
    F.lit("UK"), F.lit("GB"),
    F.lit("Australia"), F.lit("AU"),
    F.lit("Mexico"), F.lit("MX"),
    F.lit("Spain"), F.lit("ES")
])

# COMMAND ----------

# 3. Apply Transformations
silver_passenger_df = bronze_passenger_df \
    .filter(F.col("passenger_id").isNotNull()) \
    .dropDuplicates(["passenger_id"]) \
    .withColumn("full_name", F.initcap(F.trim(F.col("full_name")))) \
    .withColumn("first_name", F.split(F.col("full_name"), " ").getItem(0)) \
    .withColumn("last_name", F.split(F.col("full_name"), " ").getItem(1)) \
    .withColumn("gender", 
        F.when(F.col("gender") == "M", "Male")
         .when(F.col("gender") == "F", "Female")
         .otherwise("Other")
    ) \
    .withColumn("passenger_type", 
        F.when(F.col("age") < 2, "Infant")
         .when((F.col("age") >= 2) & (F.col("age") < 12), "Child")
         .when((F.col("age") >= 12) & (F.col("age") < 65), "Adult")
         .when(F.col("age") >= 65, "Senior")
         .otherwise("Unknown")
    ) \
    .withColumn("country", F.trim(F.col("country"))) \
    .withColumn("country_iso_code", country_iso_map[F.col("country")]) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("passenger_sk", F.sha2(F.col("passenger_id").cast("string"), 256)) 

# COMMAND ----------

display(silver_passenger_df)

# COMMAND ----------

# 4. Save to Silver as Delta
silver_passenger_df.write.format("delta").mode("overwrite").save(silver_path)