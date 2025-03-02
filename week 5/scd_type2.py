from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date

# Step 1: Initialize Spark Session
spark = SparkSession.builder.appName("SCD_Type_II").getOrCreate()

# Step 2: Simulated Existing Customer Dimension Table
existing_customers = spark.createDataFrame([
    (1, "Alice", "NY", "2023-01-01", "9999-12-31", True),
    (2, "Bob", "CA", "2023-01-01", "9999-12-31", True)
], ["customer_id", "customer_name", "state", "start_date", "end_date", "is_current"])

# Step 3: Incoming Data (New & Updated Customers)
new_customers = spark.createDataFrame([
    (1, "Alice", "TX"),   # Address change from NY → TX
    (2, "Bob", "CA"),     # No change (should not update)
    (3, "Charlie", "FL")  # New customer
], ["customer_id", "customer_name", "state"])

# Step 4: Identify Changes (Join new data with existing)
joined_data = new_customers.join(
    existing_customers, ["customer_id"], "left_outer"
).select(
    new_customers["customer_id"],
    new_customers["customer_name"],
    new_customers["state"].alias("new_state"),
    existing_customers["state"].alias("old_state"),
    existing_customers["is_current"]
)

# Step 5: Detect New & Changed Records
changed_records = joined_data.filter(
    (col("new_state") != col("old_state")) | col("old_state").isNull()
)

# Step 6: Mark Old Records as Inactive
updates_to_close = changed_records.join(existing_customers, "customer_id") \
    .filter(existing_customers["is_current"] == True) \
    .select(
        col("customer_id"),
        col("customer_name"),
        col("state"),
        col("start_date"),
        current_date().alias("end_date"),
        lit(False).alias("is_current")
    )

# Step 7: Insert New Versions for Updated Records
new_entries = changed_records.select(
    col("customer_id"),
    col("customer_name"),
    col("new_state").alias("state"),
    current_date().alias("start_date"),
    lit("9999-12-31").alias("end_date"),
    lit(True).alias("is_current")
)

# Step 8: Merge the Final Customer Table
final_customer_dim = existing_customers.filter(existing_customers["is_current"] == True) \
    .subtract(updates_to_close) \
    .union(updates_to_close) \
    .union(new_entries)

# Step 9: Show & Save Output
final_customer_dim.show()
final_customer_dim.write.mode("overwrite").csv("gs://test_iitm_bucket_gokul_gcp/scd_output")

print("\u2705 SCD Type II processing completed!")
