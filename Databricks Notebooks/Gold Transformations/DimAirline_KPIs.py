# Databricks notebook source
# MAGIC %md
# MAGIC  ==============================================================================
# MAGIC ## 🏆 Gold Layer - Airline Revenue & Performance KPIs
# MAGIC  ==============================================================================
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# COMMAND ----------

silver_path = "abfss://silver@adfkeshavstorage.dfs.core.windows.net"
gold_path = "abfss://gold@adfkeshavstorage.dfs.core.windows.net"

# COMMAND ----------

# 1. Load Silver Delta Tables
dim_airline = spark.read.format("delta").load(f"{silver_path}/DimAirline")
fact_bookings = spark.read.format("delta").load(f"{silver_path}/fact_bookings")

# COMMAND ----------

dim_airline.printSchema()
fact_bookings.printSchema()

# COMMAND ----------

# 2. Join and Aggregate
fb = fact_bookings.alias("fb")
da = dim_airline.alias("da")

airline_kpis_df = fb.join(
    da,
    F.col("fb.airline_sk") == F.col("da.airline_sk"),
    "inner"
) \
.groupBy(
    F.col("da.airline_sk"),
    F.col("da.airline_id"),
    F.col("da.airline_name"),
    F.col("da.region")
) \
.agg(
    # KPI 1: Total Revenue (using decimal/double safely)
    F.round(F.sum(F.col("fb.ticket_cost")), 2).alias("total_revenue"),
    
    # KPI 2: Total Bookings
    F.count(F.col("fb.booking_id")).alias("total_bookings"),
    
    # KPI 3: Average Ticket Price
    F.round(F.avg(F.col("fb.ticket_cost")), 2).alias("avg_ticket_price"),
    
    # KPI 4: Check-in Rate (%) -> Optimized using F.avg(col == 'Yes') 
    # Protected against division by zero if total_bookings is 0
    F.round(
        F.coalesce(F.avg(F.when(F.col("fb.checkin_status") == "Yes", 1.0).otherwise(0.0)), F.lit(0.0)) * 100, 
        2
    ).alias("checkin_rate_pct"),
    
    # KPI 5 (Additional): No-Show Rate (%)
    F.round(
        F.coalesce(F.avg(F.when(F.col("fb.checkin_status") == "No", 1.0).otherwise(0.0)), F.lit(0.0)) * 100, 
        2
    ).alias("noshow_rate_pct"),
    
    # KPI 6 (Additional): Revenue per Minute of Flight (Yield)
    F.round(
        F.when(F.sum(F.col("fb.flight_duration_mins")) > 0, 
               F.sum(F.col("fb.ticket_cost")) / F.sum(F.col("fb.flight_duration_mins")))
        .otherwise(0.0), 
        2
    ).alias("revenue_per_flight_minute")
) \
.withColumn("ingestion_timestamp", F.current_timestamp())

# COMMAND ----------

airline_kpis_df.printSchema()
display(airline_kpis_df)

# COMMAND ----------

# 3. Write to Gold Container in Delta Format (SCD Type 1 Overwrite is standard for Gold aggregate tables)
airline_kpis_df.write.format("delta").mode("overwrite").save(f"{gold_path}/AirlineKpis")

# COMMAND ----------

