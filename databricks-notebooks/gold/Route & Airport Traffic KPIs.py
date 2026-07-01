# Databricks notebook source
# MAGIC %md
# MAGIC ==============================================================================
# MAGIC ### 🏆 Gold Layer - Routes & Airport Traffic KPIs 
# MAGIC ==============================================================================
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

silver_path = "abfss://silver@adfkeshavstorage.dfs.core.windows.net"
gold_path = "abfss://gold@adfkeshavstorage.dfs.core.windows.net"

# COMMAND ----------

# 1. Load Silver Delta Tables
dim_flight = spark.read.format("delta").load(f"{silver_path}/DimFlight")
dim_airport = spark.read.format("delta").load(f"{silver_path}/DimAirport")
fact_bookings = spark.read.format("delta").load(f"{silver_path}/fact_bookings")

# COMMAND ----------

dim_flight.printSchema()
dim_airport.printSchema()
fact_bookings.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part A: Route Performance KPIs

# COMMAND ----------

# Join FactBookings with DimFlight, and DimAirport (twice: once for origin, once for destination)
routes_df = fact_bookings \
    .join(dim_flight, fact_bookings.flight_sk == dim_flight.flight_sk, "inner") \
    .join(dim_airport.alias("org"), fact_bookings.origin_airport_sk == F.col("org.airport_sk"), "inner") \
    .join(dim_airport.alias("dest"), fact_bookings.destination_airport_sk == F.col("dest.airport_sk"), "inner")
    
route_performance_df = routes_df.groupBy(
    F.col("org.airport_name").alias("origin_airport"),
    F.col("org.city").alias("origin_city"),
    F.col("dest.airport_name").alias("destination_airport"),
    F.col("dest.city").alias("destination_city")
) \
.agg(
    F.count(F.col("booking_id")).alias("total_bookings"),
    F.round(F.sum(F.col("ticket_cost")), 2).alias("total_revenue"),
    F.round(F.avg(F.col("ticket_cost")), 2).alias("avg_ticket_price"),
    F.round(F.avg(F.col("flight_duration_mins")), 0).cast("int").alias("avg_flight_duration_mins")
) \
.withColumn("ingestion_timestamp", F.current_timestamp())


# COMMAND ----------

route_performance_df.display()

# COMMAND ----------

# Write Route Performance to Gold
route_performance_df.write.format("delta").mode("overwrite").save(f"{gold_path}/route_performance_kpis")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Part B: Airport Traffic (Departures vs. Arrivals)

# COMMAND ----------

# Calculate Outbound (Departures) per Airport
departures_df = fact_bookings.groupBy("origin_airport_sk").agg(
    F.count("booking_id").alias("departing_passengers"),
    F.sum("ticket_cost").alias("departure_revenue")
)
# Calculate Inbound (Arrivals) per Airport
arrivals_df = fact_bookings.groupBy("destination_airport_sk").agg(
    F.count("booking_id").alias("arriving_passengers")
)
# Join Departures and Arrivals with DimAirport to get names and cities
airport_traffic_df = dim_airport \
    .join(departures_df, dim_airport.airport_sk == departures_df.origin_airport_sk, "left") \
    .join(arrivals_df, dim_airport.airport_sk == arrivals_df.destination_airport_sk, "left") \
    .select(
        dim_airport.airport_name,
        dim_airport.city,
        dim_airport.country,
        dim_airport.country_iso_code,
        F.coalesce(F.col("departing_passengers"), F.lit(0)).alias("departing_passengers"),
        F.coalesce(F.col("arriving_passengers"), F.lit(0)).alias("arriving_passengers"),
        # Total traffic (Inbound + Outbound)
        (F.coalesce(F.col("departing_passengers"), F.lit(0)) + 
         F.coalesce(F.col("arriving_passengers"), F.lit(0))).alias("total_passenger_traffic"),
        F.round(F.coalesce(F.col("departure_revenue"), F.lit(0.0)), 2).alias("departure_revenue")
    ) \
    .withColumn("ingestion_timestamp", F.current_timestamp())

# COMMAND ----------

display(airport_traffic_df)

# COMMAND ----------

# Write Airport Traffic to Gold
airport_traffic_df.write.format("delta").mode("overwrite").save(f"{gold_path}/airport_traffic_kpis")