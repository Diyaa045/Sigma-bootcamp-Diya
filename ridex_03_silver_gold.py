# Databricks notebook source
# ============================================================
#  SIGMOID DE BOOTCAMP — DAY 8
#  RideX Pipeline | Notebook 3 of 3
#  ridex_03_silver_gold.py
#  PURPOSE: Read Silver -> Aggregate -> Gold Delta
# ============================================================

# COMMAND ----------

# ── RECOVERY CELL — run this first every session ─────────────

YOUR_NAME = "Diya"   # must match Notebooks 1 & 2

if YOUR_NAME == "CHANGE_ME":
    raise Exception("Change YOUR_NAME to your first name before running!")

# ── FOR CORPORATE CLUSTER → change to de_workspace26
# ── FOR PERSONAL ACCOUNT  → change to devproacademytraining

CATALOG = "de_workspace26"

YOUR_DB  = f"{CATALOG}.ridex_{YOUR_NAME}"

print(f"✅ Student   : {YOUR_NAME}")
print(f"   Database  : {YOUR_DB}")
print(f"   Reading   : {YOUR_DB}.ridex_silver")
print(f"   Writing   : {YOUR_DB}.ridex_gold_city")

# COMMAND ----------

# ── STEP 1: Verify Silver exists ─────────────────────────────

silver_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {YOUR_DB}.ridex_silver"
).first()["n"]

if silver_count == 0:
    raise Exception("Silver table is empty! Run Notebook 2 first.")

print(f"✅ Silver table verified: {silver_count} rows ready")

# COMMAND ----------

# ── STEP 2: Build Gold aggregation ───────────────────────────

from pyspark.sql import functions as F

gold_df = (
    spark.read.table(f"{YOUR_DB}.ridex_silver")
        .groupBy("city")
        .agg(
            F.count("trip_id")              .alias("total_trips"),
            F.round(F.sum("fare_amount"), 2).alias("total_revenue"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
            F.round(F.avg("distance_km"), 1).alias("avg_distance_km")
        )
        .orderBy(F.desc("total_revenue"))
        .withColumn("_gold_timestamp", F.current_timestamp())
)

print("✅ Gold aggregation ready")
display(gold_df)

# COMMAND ----------

# ── STEP 3: Write Gold Delta table ───────────────────────────

spark.sql(f"DROP TABLE IF EXISTS {YOUR_DB}.ridex_gold_city")

(
    gold_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{YOUR_DB}.ridex_gold_city")
)

gold_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {YOUR_DB}.ridex_gold_city"
).first()["n"]

print(f"✅ Gold table written: {gold_count} cities")
print(f"   {YOUR_DB}.ridex_gold_city")

# COMMAND ----------

# ── STEP 4: Final business summary ───────────────────────────

display(spark.sql(f"""
  SELECT
    city,
    total_trips,
    total_revenue,
    avg_fare,
    avg_distance_km,
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank
  FROM {YOUR_DB}.ridex_gold_city
"""))

top_city = spark.sql(f"""
  SELECT city, total_revenue
  FROM {YOUR_DB}.ridex_gold_city
  ORDER BY total_revenue DESC
  LIMIT 1
""").first()

print(f"""
======================================
  RideX Pipeline Complete!
  Student      : {YOUR_NAME}
  Silver rows  : {silver_count}
  Gold cities  : {gold_count}
  Top city     : {top_city['city']}
  Top revenue  : Rs {top_city['total_revenue']:,}
  Pipeline     : CSV -> Bronze -> Silver -> Gold
  Status       : SUCCESS
======================================
""")
