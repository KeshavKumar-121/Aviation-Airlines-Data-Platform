# Databricks notebook source
# MAGIC %md
# MAGIC ## ✈️ DimAirport - Bronze to Silver Ingestion Pipeline
# MAGIC
# MAGIC #### **Overview**
# MAGIC This notebook reads raw Airport dimension data from the **Bronze** storage container, performs data cleaning, standardizes values, enriches the records with region mappings, and saves the cleaned dataset as a Delta table in the **Silver** container.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Pipeline Metadata**
# MAGIC | Attribute | Detail |
# MAGIC | :--- | :--- |
# MAGIC | **Source Path** | `abfss://bronze@adfkeshavstorage.dfs.core.windows.net/GitHub/DimAirport.json` |
# MAGIC | **Target Path** | `abfss://silver@adfkeshavstorage.dfs.core.windows.net/DimAirport` |
# MAGIC | **Format** | JSON (Bronze) $\rightarrow$ Delta (Silver) |
# MAGIC | **Target Type** | Slowly Changing Dimension (SCD) Type 1 / Lookup Table |
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Define the path for both the containers
bronze_path  = "abfss://bronze@adfkeshavstorage.dfs.core.windows.net/GitHub/DimAirport.json"
silver_path  = "abfss://silver@adfkeshavstorage.dfs.core.windows.net/DimAirport"

# COMMAND ----------

# 1. Define Schema
airport_schema = StructType([
    StructField("airport_id", IntegerType(), True),
    StructField("airport_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("country", StringType(), True)
])
# 2. Read Bronze Data (Reading JSON with multiline option)
bronze_airport_df = spark.read.option("multiline", "true").schema(airport_schema).json(bronze_path)

# COMMAND ----------

display(bronze_airport_df)

# COMMAND ----------

# Country ISO mapping dictionary
country_iso_map = F.create_map([
    F.lit("USA"), F.lit("US"),
    F.lit("UK"), F.lit("GB"),
    F.lit("India"), F.lit("IN"),
    F.lit("China"), F.lit("CN"),
    F.lit("Canada"), F.lit("CA"),
    F.lit("France"), F.lit("FR"),
    F.lit("Japan"), F.lit("JP"),
    F.lit("UAE"), F.lit("AE"),
    F.lit("Germany"), F.lit("DE"),
    F.lit("Australia"), F.lit("AU")
])

# 3. Clean, Filter, and Transform
silver_airport_df = bronze_airport_df \
    .filter(F.col("airport_id").isNotNull()) \
    .dropDuplicates(["airport_id"]) \
    .withColumn("airport_name", 
        F.trim(F.regexp_replace(F.col("airport_name"), r"\bIntl\b|\bInt'l\b", "International"))
    ) \
    .withColumn("city", F.initcap(F.trim(F.col("city")))) \
    .withColumn("country", F.trim(F.col("country"))) \
    .withColumn("country_iso_code", country_iso_map[F.col("country")]) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("airport_sk", F.sha2(F.col("airport_id").cast("string"), 256))

# COMMAND ----------

display(silver_airport_df)

# COMMAND ----------

# 4. Save to Silver as Delta
silver_airport_df.write.format("delta").mode("overwrite").save(silver_path)