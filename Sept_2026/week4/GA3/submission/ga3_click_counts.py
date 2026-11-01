"""
GA3 - Week 4: Compute click counts by time-of-day bins using PySpark
Both RDD and DataFrame implementations.

Usage (submit to Dataproc):
    gcloud dataproc jobs submit pyspark ga3_click_counts.py \
        --cluster=gokul-s-dataproc-cluster \
        --region=asia-south1 \
        --project=project-77971f5c-a874-479a-a6c \
        -- gs://YOUR_BUCKET/input/clicks.txt gs://YOUR_BUCKET/output
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, when, count
import sys


def get_time_bin(h):
    """Assign an hour (0-23) to a time-of-day bin."""
    if 0 <= h < 6:
        return "0-6"
    elif 6 <= h < 12:
        return "6-12"
    elif 12 <= h < 18:
        return "12-18"
    else:
        return "18-24"


def rdd_solution(spark, input_path, output_path):
    """RDD-based solution to count clicks by time-of-day bin."""
    sc = spark.sparkContext

    # Read the text file; first line is the header
    raw_rdd = sc.textFile(input_path)

    # Grab header so we can skip it
    header = raw_rdd.first()

    data_rdd = (
        raw_rdd
        .filter(lambda line: line != header)           # skip header
        .filter(lambda line: line.strip() != "")       # skip blank lines
        .map(lambda line: line.split(","))              # split CSV
        .filter(lambda parts: len(parts) >= 2)         # safety check
        .map(lambda parts: parts[1].strip())           # extract timestamp string
        .map(lambda ts: int(ts.split(" ")[1].split(":")[0]))  # extract hour
        .map(lambda h: (get_time_bin(h), 1))           # (bin, 1)
        .reduceByKey(lambda a, b: a + b)               # sum per bin
        .sortByKey()
    )

    # Print to driver logs
    print("\n===== RDD Solution =====")
    for bin_label, cnt in data_rdd.collect():
        print(f"{bin_label} -> {cnt}")

    # Save to GCS
    rdd_output = output_path.rstrip("/") + "/rdd_output"
    data_rdd.saveAsTextFile(rdd_output)
    print(f"RDD output saved to {rdd_output}")


def dataframe_solution(spark, input_path, output_path):
    """DataFrame-based solution to count clicks by time-of-day bin."""

    # Read as CSV with header
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Show schema for debugging
    df.printSchema()

    # Identify the timestamp column (handle possible name variations)
    ts_col = None
    for c in df.columns:
        if "timestamp" in c.lower() or "time" in c.lower():
            ts_col = c
            break
    if ts_col is None:
        # Assume second column is timestamp
        ts_col = df.columns[1]

    # Extract hour and assign bin
    df_with_bin = df.withColumn(
        "hour", hour(col(ts_col).cast("timestamp"))
    ).withColumn(
        "time_bin",
        when((col("hour") >= 0) & (col("hour") < 6), "0-6")
        .when((col("hour") >= 6) & (col("hour") < 12), "6-12")
        .when((col("hour") >= 12) & (col("hour") < 18), "12-18")
        .otherwise("18-24")
    )

    # Group by time_bin and count
    result_df = (
        df_with_bin
        .groupBy("time_bin")
        .agg(count("*").alias("click_count"))
        .orderBy("time_bin")
    )

    # Print to driver logs
    print("\n===== DataFrame Solution =====")
    result_df.show()

    # Save to GCS
    df_output = output_path.rstrip("/") + "/df_output"
    result_df.coalesce(1).write.mode("overwrite").csv(df_output, header=True)
    print(f"DataFrame output saved to {df_output}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: ga3_click_counts.py <input_path> <output_path>")
        print("  e.g.: ga3_click_counts.py gs://bucket/input/clicks.txt gs://bucket/output")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("GA3_ClickCounts").getOrCreate()

    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")

    rdd_solution(spark, input_path, output_path)
    dataframe_solution(spark, input_path, output_path)

    spark.stop()
    print("\nDone!")

