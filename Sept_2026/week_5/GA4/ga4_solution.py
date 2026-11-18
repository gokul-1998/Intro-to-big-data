"""
GA4 — PySpark DataFrame Solution on Google Cloud Dataproc
Week 5 Graded Assignment 4

Datasets (from GCS):
  - customers.csv  : customer_id, customer_name, city, status
  - transactions.csv: transaction_id, customer_id, transaction_date, transaction_amount, payment_mode

Outputs written to GCS:
  - cleaned_data/      : cleaned customers + transactions
  - invalid_rows/      : all invalid / duplicate records
  - joined_data/       : inner join on customer_id
  - aggregates/        : per-customer and per-city aggregations

Usage (submit to Dataproc):
  gcloud dataproc jobs submit pyspark \
      gs://<BUCKET>/ga4_solution.py \
      --cluster=<CLUSTER> \
      --region=<REGION> \
      --project=<PROJECT> \
      -- gs://<BUCKET>/input gs://<BUCKET>/output
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window

# ---------------------------------------------------------------------------
# 0. Bootstrap
# ---------------------------------------------------------------------------
if len(sys.argv) != 3:
    print("Usage: ga4_solution.py <input_gcs_path> <output_gcs_path>")
    sys.exit(1)

INPUT  = sys.argv[1].rstrip("/")   # e.g. gs://my-bucket/input
OUTPUT = sys.argv[2].rstrip("/")   # e.g. gs://my-bucket/output

spark = (
    SparkSession.builder
    .appName("GA4-DataFrame-Solution")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("GA4: PySpark DataFrame Solution")
print(f"  Input  : {INPUT}")
print(f"  Output : {OUTPUT}")
print("=" * 60)

# ---------------------------------------------------------------------------
# 1. Load & Inspect
# ---------------------------------------------------------------------------
print("\n--- 1. Load & Inspect ---")

customers_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{INPUT}/customers.csv")
)
transactions_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{INPUT}/transactions.csv")
)

print("\n[Customers] Schema:")
customers_raw.printSchema()
print("[Customers] Sample rows:")
customers_raw.show(5, truncate=False)

print("\n[Transactions] Schema:")
transactions_raw.printSchema()
print("[Transactions] Sample rows:")
transactions_raw.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 2. Column Transformations (applied before cleaning so we can validate dates)
# ---------------------------------------------------------------------------
print("\n--- 2. Column Transformations ---")

# --- Customers: standardize city names (title-case + known aliases) ---
city_map = {
    "kochi":   "Kochi",
    "cochin":  "Kochi",
    "KOCHI":   "Kochi",
    "COCHIN":  "Kochi",
    "mumbai":  "Mumbai",
    "MUMBAI":  "Mumbai",
    "bombay":  "Mumbai",
    "BOMBAY":  "Mumbai",
    "delhi":   "Delhi",
    "DELHI":   "Delhi",
    "new delhi": "Delhi",
    "NEW DELHI": "Delhi",
    "bangalore":  "Bangalore",
    "BANGALORE":  "Bangalore",
    "bengaluru":  "Bangalore",
    "BENGALURU":  "Bangalore",
    "chennai":    "Chennai",
    "CHENNAI":    "Chennai",
    "madras":     "Chennai",
    "MADRAS":     "Chennai",
    "hyderabad":  "Hyderabad",
    "HYDERABAD":  "Hyderabad",
    "kolkata":    "Kolkata",
    "KOLKATA":    "Kolkata",
    "calcutta":   "Kolkata",
    "CALCUTTA":   "Kolkata",
    "pune":       "Pune",
    "PUNE":       "Pune",
    "ahmedabad":  "Ahmedabad",
    "AHMEDABAD":  "Ahmedabad",
}
# Build a CASE-WHEN expression from the mapping
city_expr = F.trim(F.lower(F.col("city")))
city_when = None
for raw, clean in city_map.items():
    cond = F.trim(F.lower(F.col("city"))) == raw.lower()
    city_when = (
        F.when(cond, clean)
        if city_when is None
        else city_when.when(cond, clean)
    )
# Fall back to initcap for any city not in the map
city_standardized = city_when.otherwise(F.initcap(F.trim(F.col("city"))))

customers_transformed = customers_raw.withColumn("city", city_standardized)

# --- Transactions: cast types, parse date, add derived columns ---
transactions_transformed = (
    transactions_raw
    # Ensure transaction_amount is numeric
    .withColumn("transaction_amount", F.col("transaction_amount").cast(DoubleType()))
    # Parse date (handle multiple possible formats gracefully)
    .withColumn(
        "transaction_date_parsed",
        F.coalesce(
            F.to_date(F.col("transaction_date"), "yyyy-MM-dd"),
            F.to_date(F.col("transaction_date"), "dd-MM-yyyy"),
            F.to_date(F.col("transaction_date"), "MM/dd/yyyy"),
        )
    )
    # Overwrite transaction_date with normalised YYYY-MM-DD string
    .withColumn(
        "transaction_date",
        F.date_format(F.col("transaction_date_parsed"), "yyyy-MM-dd")
    )
    .drop("transaction_date_parsed")
    # Month extraction
    .withColumn(
        "transaction_month",
        F.month(F.to_date(F.col("transaction_date"), "yyyy-MM-dd"))
    )
    # Amount category
    .withColumn(
        "amount_category",
        F.when(F.col("transaction_amount") < 500,  "Low")
         .when(F.col("transaction_amount") < 2000, "Medium")
         .otherwise("High")
    )
)

print("[Transactions] After transformation:")
transactions_transformed.show(5, truncate=False)
print("[Customers] After city standardization:")
customers_transformed.show(5, truncate=False)

# ---------------------------------------------------------------------------
# 3. Data Cleaning
# ---------------------------------------------------------------------------
print("\n--- 3. Data Cleaning ---")

# ---- 3a. Clean Customers ----

# Flag invalid customer rows
customers_invalid_flag = customers_transformed.withColumn(
    "_invalid_reason",
    F.when(
        F.col("customer_id").isNull() | (F.trim(F.col("customer_id").cast("string")) == ""),
        "missing customer_id"
    ).when(
        F.col("city").isNull() | (F.trim(F.col("city")) == ""),
        "missing city"
    ).when(
        F.col("status").isNull() | (F.trim(F.col("status")) == ""),
        "missing status"
    ).when(
        F.col("customer_name").isNull() | (F.trim(F.col("customer_name")) == ""),
        "missing customer_name"
    ).otherwise(None)
)

# Duplicates: flag rows where full-row duplicate, keeping first
customer_window = Window.partitionBy(
    customers_transformed.columns
).orderBy(F.lit(1))

customers_dup_flag = customers_invalid_flag.withColumn(
    "_row_num", F.row_number().over(customer_window)
).withColumn(
    "_invalid_reason",
    F.when(
        (F.col("_invalid_reason").isNull()) & (F.col("_row_num") > 1),
        "duplicate row"
    ).otherwise(F.col("_invalid_reason"))
)

invalid_customers = (
    customers_dup_flag
    .filter(F.col("_invalid_reason").isNotNull())
    .drop("_row_num")
)
cleaned_customers = (
    customers_dup_flag
    .filter(F.col("_invalid_reason").isNull())
    .drop("_invalid_reason", "_row_num")
)

print("[Customers] Invalid rows:")
invalid_customers.show(10, truncate=False)
print(f"[Customers] Invalid count : {invalid_customers.count()}")
print(f"[Customers] Cleaned count : {cleaned_customers.count()}")

# ---- 3b. Clean Transactions ----

transactions_invalid_flag = transactions_transformed.withColumn(
    "_invalid_reason",
    F.when(
        F.col("transaction_id").isNull() | (F.trim(F.col("transaction_id").cast("string")) == ""),
        "missing transaction_id"
    ).when(
        F.col("customer_id").isNull() | (F.trim(F.col("customer_id").cast("string")) == ""),
        "missing customer_id"
    ).when(
        F.col("transaction_amount").isNull() | (F.col("transaction_amount") <= 0),
        "invalid transaction_amount"
    ).when(
        F.col("transaction_date").isNull() | (F.trim(F.col("transaction_date")) == ""),
        "invalid transaction_date"
    ).when(
        F.col("payment_mode").isNull() | (F.trim(F.col("payment_mode")) == ""),
        "missing payment_mode"
    ).otherwise(None)
)

# Referential integrity: customer_id must exist in cleaned customers
valid_customer_ids = cleaned_customers.select(
    F.col("customer_id").alias("_cid")
)
transactions_ref_flag = (
    transactions_invalid_flag
    .join(valid_customer_ids, transactions_invalid_flag.customer_id == valid_customer_ids._cid, "left")
    .withColumn(
        "_invalid_reason",
        F.when(
            F.col("_invalid_reason").isNull() & F.col("_cid").isNull(),
            "customer_id not in customer dataset"
        ).otherwise(F.col("_invalid_reason"))
    )
    .drop("_cid")
)

# Duplicate transactions (all columns match)
txn_window = Window.partitionBy(
    transactions_transformed.columns
).orderBy(F.lit(1))

transactions_dup_flag = transactions_ref_flag.withColumn(
    "_row_num", F.row_number().over(txn_window)
).withColumn(
    "_invalid_reason",
    F.when(
        F.col("_invalid_reason").isNull() & (F.col("_row_num") > 1),
        "duplicate row"
    ).otherwise(F.col("_invalid_reason"))
)

invalid_transactions = (
    transactions_dup_flag
    .filter(F.col("_invalid_reason").isNotNull())
    .drop("_row_num")
)
cleaned_transactions = (
    transactions_dup_flag
    .filter(F.col("_invalid_reason").isNull())
    .drop("_invalid_reason", "_row_num")
)

print("[Transactions] Invalid rows:")
invalid_transactions.show(10, truncate=False)
print(f"[Transactions] Invalid count : {invalid_transactions.count()}")
print(f"[Transactions] Cleaned count : {cleaned_transactions.count()}")

# ---- Combine all invalid rows into a single DataFrame ----
# Align schemas by adding missing columns as nulls
customer_cols   = set(invalid_customers.columns)
txn_cols        = set(invalid_transactions.columns)

for c in txn_cols - customer_cols:
    invalid_customers = invalid_customers.withColumn(c, F.lit(None).cast("string"))
for c in customer_cols - txn_cols:
    invalid_transactions = invalid_transactions.withColumn(c, F.lit(None).cast("string"))

# Reorder to same column order
all_cols = sorted(invalid_customers.columns)
invalid_rows = (
    invalid_customers.select(all_cols)
    .unionByName(invalid_transactions.select(all_cols))
)

print("[All] Combined invalid rows sample:")
invalid_rows.show(10, truncate=False)

# Save invalid rows
invalid_rows.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT}/invalid_rows")
print(f"Saved invalid_rows → {OUTPUT}/invalid_rows")

# Save cleaned data (save both cleaned datasets)
cleaned_customers.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT}/cleaned_data/customers")
cleaned_transactions.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT}/cleaned_data/transactions")
print(f"Saved cleaned_data → {OUTPUT}/cleaned_data/")

# ---------------------------------------------------------------------------
# 4. Join
# ---------------------------------------------------------------------------
print("\n--- 4. Join ---")

joined_data = cleaned_transactions.join(
    cleaned_customers,
    on="customer_id",
    how="inner"
)

print("[Joined] Sample rows:")
joined_data.show(10, truncate=False)
print(f"[Joined] Row count: {joined_data.count()}")

joined_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT}/joined_data")
print(f"Saved joined_data → {OUTPUT}/joined_data")

# ---------------------------------------------------------------------------
# 5. Aggregations
# ---------------------------------------------------------------------------
print("\n--- 5. Aggregations ---")

# 5a. Total & average transaction amount per customer
per_customer = (
    joined_data
    .groupBy("customer_id", "customer_name")
    .agg(
        F.round(F.sum("transaction_amount"),   2).alias("total_amount"),
        F.round(F.avg("transaction_amount"),   2).alias("avg_amount"),
        F.count("transaction_id")               .alias("txn_count"),
    )
    .orderBy(F.desc("total_amount"))
)
print("[Aggregation] Per customer:")
per_customer.show(10, truncate=False)

# 5b. Total transaction amount per city
per_city = (
    joined_data
    .groupBy("city")
    .agg(
        F.round(F.sum("transaction_amount"), 2).alias("total_amount"),
        F.count("transaction_id")              .alias("txn_count"),
    )
    .orderBy(F.desc("total_amount"))
)
print("[Aggregation] Per city:")
per_city.show(20, truncate=False)

# 5c. Top 3 customers by total transaction amount
top3_customers = per_customer.limit(3)
print("[Aggregation] Top 3 customers:")
top3_customers.show(truncate=False)

# Save aggregates
per_customer .coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT}/aggregates/per_customer")
per_city     .coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT}/aggregates/per_city")
top3_customers.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{OUTPUT}/aggregates/top3_customers")
print(f"Saved aggregates → {OUTPUT}/aggregates/")

# ---------------------------------------------------------------------------
# 6. Summary log
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("GA4 — Execution Summary")
print("=" * 60)
print(f"  Customers  — raw: {customers_raw.count()}, "
      f"cleaned: {cleaned_customers.count()}, "
      f"invalid: {invalid_customers.count()}")
print(f"  Transactions — raw: {transactions_raw.count()}, "
      f"cleaned: {cleaned_transactions.count()}, "
      f"invalid: {invalid_transactions.count()}")
print(f"  Joined rows      : {joined_data.count()}")
print(f"\n  Outputs in       : {OUTPUT}/")
print("    cleaned_data/customers/")
print("    cleaned_data/transactions/")
print("    invalid_rows/")
print("    joined_data/")
print("    aggregates/per_customer/")
print("    aggregates/per_city/")
print("    aggregates/top3_customers/")
print("=" * 60)

spark.stop()
