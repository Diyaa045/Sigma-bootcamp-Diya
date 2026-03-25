# Databricks notebook source
# ============================================================
#  SIGMOID DE BOOTCAMP — DAY 8
#  RideX Pipeline | Notebook 2 of 3
#  ridex_02_bronze_silver.py
#  PURPOSE: Read CSV from Volume -> Bronze Delta -> Silver Delta
# ============================================================

# COMMAND ----------

# ── RECOVERY CELL — run this first every session ─────────────

YOUR_NAME = "Diya"   # must match Notebook 1

if YOUR_NAME == "CHANGE_ME":
    raise Exception("Change YOUR_NAME to your first name before running!")

# ── FOR CORPORATE CLUSTER → change to de_workspace26
# ── FOR PERSONAL ACCOUNT  → change to devproacademytraining

CATALOG = "de_workspace26"

YOUR_DB      = f"{CATALOG}.ridex_{YOUR_NAME}"
LANDING_PATH = f"/Volumes/{CATALOG}/ridex_{YOUR_NAME}/landing/"

print(f"✅ Student   : {YOUR_NAME}")
print(f"   Database  : {YOUR_DB}")
print(f"   Source    : {LANDING_PATH}")

# COMMAND ----------

# ── STEP 1: Verify landing file exists ───────────────────────

import os

VOLUME_FILE = f"{LANDING_PATH}ridex_trips.csv"

if not os.path.exists(VOLUME_FILE):
    raise Exception(f"CSV not found at {VOLUME_FILE} — run Notebook 1 first!")

print(f"✅ Source file confirmed: {VOLUME_FILE}")

# COMMAND ----------

# ── STEP 2: Read CSV into Spark ──────────────────────────────

from pyspark.sql import functions as F

raw_df = (
    spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(LANDING_PATH)
        .withColumn("_load_timestamp", F.current_timestamp())
)

print(f"✅ CSV loaded: {raw_df.count()} rows")
raw_df.printSchema()

# COMMAND ----------

# ── STEP 3: Write Bronze Delta table ─────────────────────────

spark.sql(f"DROP TABLE IF EXISTS {YOUR_DB}.ridex_bronze")

(
    raw_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{YOUR_DB}.ridex_bronze")
)

bronze_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {YOUR_DB}.ridex_bronze"
).first()["n"]

print(f"✅ Bronze table written: {bronze_count} rows")
print(f"   {YOUR_DB}.ridex_bronze")

# COMMAND ----------

# ── STEP 4: Show null summary ────────────────────────────────

print("=== NULL SUMMARY in Bronze (raw data) ===")
display(spark.sql(f"""
  SELECT
    COUNT(*)                           AS total_rows,
    COUNT(*) - COUNT(fare_amount)      AS null_fare,
    COUNT(*) - COUNT(status)           AS null_status
  FROM {YOUR_DB}.ridex_bronze
"""))

# COMMAND ----------

# ── STEP 5: Clean and write Silver ───────────────────────────

silver_df = (
    spark.read.table(f"{YOUR_DB}.ridex_bronze")
        .filter(F.col("fare_amount").isNotNull())
        .filter(F.col("status").isNotNull())
        .filter(F.col("status") == "completed")
        .withColumn("trip_date",
                    F.to_date("trip_date", "yyyy-MM-dd"))
        .withColumn("fare_amount",
                    F.col("fare_amount").cast("double"))
        .withColumn("distance_km",
                    F.col("distance_km").cast("double"))
        .withColumn("_silver_timestamp", F.current_timestamp())
        .drop("_load_timestamp")
)

spark.sql(f"DROP TABLE IF EXISTS {YOUR_DB}.ridex_silver")

(
    silver_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"{YOUR_DB}.ridex_silver")
)

silver_count = spark.sql(
    f"SELECT COUNT(*) AS n FROM {YOUR_DB}.ridex_silver"
).first()["n"]

print(f"✅ Silver table written: {silver_count} rows")
print(f"   Dropped : {bronze_count - silver_count} rows")
print(f"   {YOUR_DB}.ridex_silver")

# COMMAND ----------

# ── STEP 6: Quick verify ─────────────────────────────────────

display(spark.sql(f"""
  SELECT city,
         COUNT(*)                   AS trips,
         ROUND(SUM(fare_amount), 2) AS revenue,
         ROUND(AVG(distance_km), 1) AS avg_km
  FROM {YOUR_DB}.ridex_silver
  GROUP BY city
  ORDER BY revenue DESC
"""))

print(f"""
Bronze -> Silver Complete
Student  : {YOUR_NAME}
Bronze   : {bronze_count} rows (raw)
Silver   : {silver_count} rows (clean)
Dropped  : {bronze_count - silver_count} rows
""")
