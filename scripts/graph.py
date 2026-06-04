import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, avg

# -----------------------------
# Config
# -----------------------------
# Use environment variables if provided, otherwise default to local mode
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
DATA_PATH = os.getenv("DATA_PATH", "datasets/Electric_Vehicle_Population_Data.csv")
APP_NAME = "ElectricVehicleAnalytics"

# Optional: remove broken SPARK_HOME if it points to a nonexistent location
spark_home = os.getenv("SPARK_HOME")
if spark_home and not os.path.exists(spark_home):
    print(f"Warning: SPARK_HOME='{spark_home}' does not exist. Ignoring it.")
    os.environ.pop("SPARK_HOME", None)

# -----------------------------
# Create Spark session
# -----------------------------
spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName(APP_NAME) \
    .getOrCreate()

# -----------------------------
# Load CSV
# -----------------------------
df = spark.read.csv(
    DATA_PATH,
    header=True,
    inferSchema=True
)

# -----------------------------
# Function to clean column names
# -----------------------------
def clean_column_name(col_name):
    col_name = col_name.strip().lower()
    col_name = re.sub(r'[^a-zA-Z0-9]+', '_', col_name)  # replace spaces/special chars with _
    col_name = re.sub(r'_+', '_', col_name)             # remove duplicate underscores
    col_name = col_name.strip('_')                      # remove leading/trailing _
    return col_name

# Rename columns
new_columns = [clean_column_name(c) for c in df.columns]
df = df.toDF(*new_columns)

# -----------------------------
# Basic info
# -----------------------------
print("Rows:", df.count())
print("Columns:", df.columns)

# Show sample data
df.show(10, truncate=False)

# -----------------------------
# Example analytics
# -----------------------------

# 1. Count vehicles by state
if "state" in df.columns:
    print("\nTop states by number of vehicles:")
    df.groupBy("state").count().orderBy(desc("count")).show()

# 2. Count vehicles by city
if "city" in df.columns:
    print("\nTop cities by number of vehicles:")
    df.groupBy("city").count().orderBy(desc("count")).show(10)

# 3. Count vehicles by make
if "make" in df.columns:
    print("\nTop vehicle makes:")
    df.groupBy("make").count().orderBy(desc("count")).show(10)

# 4. Count vehicles by model year
if "model_year" in df.columns:
    print("\nVehicles by model year:")
    df.groupBy("model_year").count().orderBy(desc("count")).show()

# 5. Count by electric vehicle type
if "electric_vehicle_type" in df.columns:
    print("\nVehicle count by electric vehicle type:")
    df.groupBy("electric_vehicle_type").count().orderBy(desc("count")).show()

# 6. Count by CAFV eligibility
if "clean_alternative_fuel_vehicle_cafv_eligibility" in df.columns:
    print("\nCAFV eligibility distribution:")
    df.groupBy("clean_alternative_fuel_vehicle_cafv_eligibility") \
      .count() \
      .orderBy(desc("count")) \
      .show()

# 7. Average electric range by make
if "make" in df.columns and "electric_range" in df.columns:
    print("\nAverage electric range by make:")
    df.groupBy("make") \
      .agg(avg("electric_range").alias("avg_electric_range")) \
      .orderBy(desc("avg_electric_range")) \
      .show(10)

spark.stop()
