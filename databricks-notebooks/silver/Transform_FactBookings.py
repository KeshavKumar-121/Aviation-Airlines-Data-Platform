# Databricks notebook source
# MAGIC %md
# MAGIC ## ✈️ FactBookings - Bronze to Silver Incremental Ingestion
# MAGIC ### Pattern: Auto Loader + Delta Merge (Structured Streaming Upsert)
# MAGIC
# MAGIC #### **Overview**
# MAGIC This notebook reads raw Bookings Fact data from the **Bronze** storage container, performs data cleaning, standardizes values, enriches the records with region mappings, and saves the cleaned dataset as a Delta table in the **Silver** container.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC #### **Pipeline Metadata**
# MAGIC | Attribute | Detail |
# MAGIC | :--- | :--- |
# MAGIC | **Source Path** | `abfss://bronze@adfkeshavstorage.dfs.core.windows.net/SQL/` |
# MAGIC | **Target Path** | `abfss://silver@adfkeshavstorage.dfs.core.windows.net/` |
# MAGIC | **Format** | Parquet (Bronze) $\rightarrow$ Delta (Silver) |
# MAGIC | **Target Type** | Slowly Changing Dimension (SCD) Type 1 / Lookup Table |
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, DateType
from delta.tables import DeltaTable

# COMMAND ----------

bronze_path = "abfss://bronze@adfkeshavstorage.dfs.core.windows.net/SQL"
silver_path = "abfss://silver@adfkeshavstorage.dfs.core.windows.net/fact_bookings"
checkpoint_path = "abfss://silver@adfkeshavstorage.dfs.core.windows.net/_checkpoints/fact_bookings"

# COMMAND ----------

# 1. Define Schema matching Azure SQL FactBookings
bookings_schema = StructType([
    StructField("booking_id", IntegerType(), True),
    StructField("passenger_id", IntegerType(), True),
    StructField("flight_id", IntegerType(), True),
    StructField("airline_id", IntegerType(), True),
    StructField("origin_airport_id", IntegerType(), True),
    StructField("destination_airport_id", IntegerType(), True),
    StructField("booking_date", DateType(), True),
    StructField("ticket_cost", DecimalType(10, 2), True), 
    StructField("flight_duration_mins", IntegerType(), True),
    StructField("checkin_status", StringType(), True)
])

# 2. Read INCREMENTALLY from Bronze using Auto Loader
incremental_df = spark.readStream.format("cloudFiles") \
    .option("cloudFiles.format", "parquet") \
    .schema(bookings_schema) \
    .load(bronze_path)


# COMMAND ----------

# 3. Clean and Transform Data (Standardization & Surrogate Key Generation)
transformed_df = incremental_df \
    .filter(F.col("booking_id").isNotNull()) \
    .withColumn("checkin_status", F.initcap(F.trim(F.col("checkin_status")))) \
    .withColumn("ingestion_timestamp", F.current_timestamp()) \
    .withColumn("booking_sk", F.sha2(F.col("booking_id").cast("string"), 256)) \
    .withColumn("passenger_sk", F.sha2(F.col("passenger_id").cast("string"), 256)) \
    .withColumn("flight_sk", F.sha2(F.col("flight_id").cast("string"), 256)) \
    .withColumn("airline_sk", F.sha2(F.col("airline_id").cast("string"), 256)) \
    .withColumn("origin_airport_sk", F.sha2(F.col("origin_airport_id").cast("string"), 256)) \
    .withColumn("destination_airport_sk", F.sha2(F.col("destination_airport_id").cast("string"), 256))

# COMMAND ----------

# 4. Define the Upsert/Merge function for each incoming batch of files
def upsert_to_silver(batch_df, batch_id):
    # Deduplicate within the incoming batch itself (in case source had duplicate bookings)
    clean_batch_df = batch_df.dropDuplicates(["booking_id"])
    
    if DeltaTable.isDeltaTable(spark, silver_path):
        # Table exists -> Perform Merge (Upsert)
        silver_table = DeltaTable.forPath(spark, silver_path)
        silver_table.alias("target").merge(
            source = clean_batch_df.alias("source"),
            condition = "target.booking_id = source.booking_id"
        ) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()
        print(f"Batch {batch_id}: Successfully merged incoming data into Silver.")
    else:
        # Table does not exist -> Initial Write (Creates the Delta Table)
        clean_batch_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(silver_path)
        print(f"Batch {batch_id}: Initialized Silver Delta table.")

# COMMAND ----------

# 5. Write incrementally using the upsert function 
# Note: trigger(availableNow=True) processes all pending files and stops the stream,
query = transformed_df.writeStream \
    .format("delta") \
    .foreachBatch(upsert_to_silver) \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(availableNow=True) \
    .start()

# COMMAND ----------

# Read the Silver table you just wrote
silver_df = spark.read.format("delta").load("abfss://silver@adfkeshavstorage.dfs.core.windows.net/fact_bookings")
# Display the rows
display(silver_df)
# Print the count of records loaded
print(f"Total records in Silver: {silver_df.count()}")
