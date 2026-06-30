# Databricks notebook source
# MAGIC %md
# MAGIC ==============================================================================
# MAGIC ### 🏆 Gold Layer - Passenger Demographics & Spend KPIs 
# MAGIC ==============================================================================

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

silver_path = "abfss://silver@adfkeshavstorage.dfs.core.windows.net"
gold_path = "abfss://gold@adfkeshavstorage.dfs.core.windows.net"

# COMMAND ----------

# 1. Load Silver Delta Tables
dim_passenger = spark.read.format("delta").load(f"{silver_path}/DimPassenger")
fact_bookings = spark.read.format("delta").load(f"{silver_path}/fact_bookings")

# COMMAND ----------

dim_passenger.printSchema()
fact_bookings.printSchema()

# COMMAND ----------

# 2. Join and Aggregate by Demographic Attributes
passenger_kpis_df = fact_bookings.join(
    dim_passenger,
    fact_bookings.passenger_sk == dim_passenger.passenger_sk,
    "inner"
) \
.groupBy(
    dim_passenger.passenger_type,
    dim_passenger.gender,
    dim_passenger.country,
    dim_passenger.country_iso_code
) \
.agg(
    # Numerator & Denominator for Check-in Rate
    F.count(F.col("booking_id")).alias("total_bookings"),
    F.sum(F.when(F.col("checkin_status") == "Yes", 1).otherwise(0)).alias("checked_in_bookings"),
    F.sum(F.when(F.col("checkin_status") == "No", 1).otherwise(0)).alias("noshow_bookings"),
    
    # Other metrics
    F.round(F.sum(F.col("ticket_cost")), 2).alias("total_revenue"),
    F.countDistinct(dim_passenger.passenger_sk).alias("unique_passengers_count"),
    F.round(F.avg(dim_passenger.age), 1).alias("avg_passenger_age"),
    F.round(F.avg(F.col("ticket_cost")), 2).alias("avg_spend_per_booking")
) \
.withColumn("ingestion_timestamp", F.current_timestamp())

# COMMAND ----------

passenger_kpis_df.printSchema()
display(passenger_kpis_df)

# COMMAND ----------

# 3. Write to Gold Container
passenger_kpis_df.write.format("delta").mode("overwrite").save(f"{gold_path}/PassengerKpis")