# Databricks notebook source
# ============================================================
#  SIGMOID DE BOOTCAMP — DAY 8
#  RideX Pipeline | Notebook 1 of 3
#  ridex_01_generate_data.py
#  PURPOSE: Generate fake RideX trip CSV → Unity Catalog Volume
# ============================================================

# COMMAND ----------

# ── RECOVERY CELL — run this first every session ─────────────

YOUR_NAME = "Diya"   # e.g. "anil" — lowercase, no spaces

if YOUR_NAME == "CHANGE_ME":
    raise Exception("Change YOUR_NAME to your first name before running!")

# ── FOR CORPORATE CLUSTER → change to de_workspace26
# ── FOR PERSONAL ACCOUNT  → change to devproacademytraining
CATALOG = "de_workspace26"

YOUR_DB      = f"{CATALOG}.ridex_{YOUR_NAME}"
LANDING_PATH = f"/Volumes/{CATALOG}/ridex_{YOUR_NAME}/landing/"

print(f"✅ Student   : {YOUR_NAME}")
print(f"   Catalog   : {CATALOG}")
print(f"   Database  : {YOUR_DB}")
print(f"   Landing   : {LANDING_PATH}")

# COMMAND ----------

# ── STEP 1: Create schema and Volume ─────────────────────────

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {YOUR_DB}")

spark.sql(f"""
  CREATE VOLUME IF NOT EXISTS {YOUR_DB}.landing
""")

print(f"✅ Schema and Volume ready")
print(f"   {YOUR_DB}.landing")

# COMMAND ----------

# ── STEP 2: Generate RideX trip data ─────────────────────────

import csv, random, uuid, shutil
from datetime import datetime, timedelta

CITIES   = ["Mumbai", "Delhi", "Bengaluru", "Chennai", "Hyderabad", "Pune"]
STATUSES = ["completed", "completed", "completed", "cancelled", "cancelled"]

random.seed(42)

def generate_ridex_trips(n=200):
    trips = []
    base  = datetime(2024, 1, 1)
    for i in range(n):
        trip_date    = base + timedelta(days=random.randint(0, 89))
        distance_km  = round(random.uniform(1.5, 45.0), 2)
        fare_amount  = round(distance_km * random.uniform(12, 18), 2)
        status       = random.choice(STATUSES)

        # Intentionally inject 10 nulls for training purposes
        if i % 20 == 0:
            fare_amount = None
            status      = None

        trips.append({
            "trip_id"     : str(uuid.uuid4())[:8].upper(),
            "driver_id"   : f"DRV{random.randint(100,999)}",
            "city"        : random.choice(CITIES),
            "distance_km" : distance_km,
            "fare_amount" : fare_amount,
            "status"      : status,
            "trip_date"   : trip_date.strftime("%Y-%m-%d")
        })
    return trips

trips = generate_ridex_trips(200)
print(f"✅ Generated {len(trips)} trip records")
print(f"   Sample: {trips[1]}")

# COMMAND ----------

# ── STEP 3: Write CSV to Unity Catalog Volume ─────────────────

import csv, shutil

LOCAL_FILE  = f"/tmp/ridex_trips_{YOUR_NAME}.csv"
VOLUME_FILE = f"{LANDING_PATH}ridex_trips.csv"

fieldnames = ["trip_id","driver_id","city","distance_km",
              "fare_amount","status","trip_date"]

# Write to local /tmp first
with open(LOCAL_FILE, "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(trips)

# Copy to Volume using shutil (Volumes = regular filesystem path)
shutil.copy(LOCAL_FILE, VOLUME_FILE)

print(f"✅ CSV written to Volume: {VOLUME_FILE}")

# COMMAND ----------

# ── STEP 4: Verify the file landed correctly ──────────────────

import os
size = os.path.getsize(VOLUME_FILE)
print(f"✅ File confirmed in Volume")
print(f"   Path : {VOLUME_FILE}")
print(f"   Size : {size:,} bytes")

# Quick peek
preview = spark.read.option("header", True).csv(LANDING_PATH)
print(f"   Rows : {preview.count()} (including header)")
display(preview.limit(5))
