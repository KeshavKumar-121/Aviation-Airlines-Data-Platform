# Databricks notebook source
# MAGIC %md
# MAGIC ## ✈️ DimAirline - Bronze to Silver Ingestion Pipeline
# MAGIC
# MAGIC #### **Overview**
# MAGIC This notebook reads raw airline dimension data from the **Bronze** storage container, performs data cleaning, standardizes values, enriches the records with region mappings, and saves the cleaned dataset as a Delta table in the **Silver** container.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Pipeline Metadata**
# MAGIC | Attribute | Detail |
# MAGIC | :--- | :--- |
# MAGIC | **Source Path** | `abfss://bronze@adfkeshavstorage.dfs.core.windows.net/onprem/DimAirline.csv` |
# MAGIC | **Target Path** | `abfss://silver@adfkeshavstorage.dfs.core.windows.net/DimAirline` |
# MAGIC | **Format** | CSV (Bronze) $\rightarrow$ Delta (Silver) |
# MAGIC | **Target Type** | Overwrite (SCD Type 1 / Lookup Table) |
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

# Define the path for both the containers
bronze_path  = "abfss://bronze@adfkeshavstorage.dfs.core.windows.net/onprem/DimAirline.csv"
silver_path  = "abfss://silver@adfkeshavstorage.dfs.core.windows.net/DimAirline"

# COMMAND ----------

# Define the schema
schema = StructType([
    StructField("airline_id", IntegerType(), True),
    StructField("airline_name", StringType(), True),
    StructField("country", StringType(), True)
])
# read the data
bronze_df = spark.read.option("header", "true").schema(schema).csv(bronze_path)

# COMMAND ----------

display(bronze_df)

# COMMAND ----------

# ISO mapping 
country_region_mapping = {
    "USA": ("US", "North America"),
    "India": ("IN", "Asia"),
    "Australia": ("AU", "Oceania"),
    "Germany": ("DE", "Europe"),
    "France": ("FR", "Europe"),
    "UAE": ("AE", "Asia"),
    "UK": ("GB", "Europe"),
    "Singapore": ("SG", "Asia"),
    "Hong Kong": ("HK", "Asia")
}

# Create SQL mapping expressions
iso_map_expr = F.create_map([F.lit(x) for x in sum([[k, v[0]] for k, v in country_region_mapping.items()], [])])
region_map_expr = F.create_map([F.lit(x) for x in sum([[k, v[1]] for k, v in country_region_mapping.items()], [])])

# COMMAND ----------

# Apply transformations
silver_df = bronze_df \
    .filter(F.col("airline_id").isNotNull())\
    .dropDuplicates(["airline_id"])\
    .withColumn("airline_name", F.initcap(F.trim(F.col("airline_name")))) \
    .withColumn("country", F.trim(F.col("country")))\
    .withColumn("country_iso_code", iso_map_expr[F.col("country")]) \
    .withColumn("region", region_map_expr[F.col("country")]) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("airline_sk", F.sha2(F.col("airline_id").cast("string"), 256)) # Surrogate Key

# COMMAND ----------

display(silver_df)

# COMMAND ----------

silver_df.write.format("delta").mode("overwrite").save(silver_path)