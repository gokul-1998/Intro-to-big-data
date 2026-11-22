"""
OPPE 1 - Train Stoppage Patterns and Platform Utilization Analysis
Course: Introduction to Big Data

PySpark batch processing on Google Cloud Dataproc:
- Section A: Stop Duration Statistics (min, max, avg, 90th percentile, Top 5 stations)
- Section B: Station Load Analysis (total stop, max > 3 * avg, 2-hour sliding window)
- Section C: Save aggregated results as CSV to GCS
"""

import sys
import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    unix_timestamp,
    when,
    min,
    max,
    avg,
    sum,
    round,
    count,
    expr,
    lit
)
from pyspark.sql.window import Window


def create_spark_session(app_name="TrainStoppageAnalysis"):
    """Initializes and returns a SparkSession."""
    return SparkSession.builder \
        .appName(app_name) \
        .getOrCreate()


def clean_and_prepare_data(spark, input_path):
    """
    Reads the CSV dataset, handles missing/malformed rows, and computes stop durations.
    """
    print(f"Reading dataset from: {input_path}")
    raw_df = spark.read.option("header", "true").option("inferSchema", "false").csv(input_path)

    # Clean whitespace from column names if present
    for col_name in raw_df.columns:
        raw_df = raw_df.withColumnRenamed(col_name, col_name.strip())

    # 1. Filter malformed rows:
    # - SEQ must be numeric
    # - Arrival time and Departure Time must be non-null and match HH:mm:ss format
    # - Station Code must be non-null
    cleaned_df = raw_df.filter(
        col("SEQ").rlike("^[0-9]+$") &
        col("Station Code").isNotNull() &
        col("Arrival time").isNotNull() &
        col("Departure Time").isNotNull() &
        col("Arrival time").rlike("^[0-9]{2}:[0-9]{2}:[0-9]{2}$") &
        col("Departure Time").rlike("^[0-9]{2}:[0-9]{2}:[0-9]{2}$")
    )

    # 2. Filter out Origin and Destination placeholder rows:
    # In Indian Railways data, 00:00:00 indicates train origin (no arrival) or train terminus (no departure).
    # Actual stoppages require valid arrival and departure.
    stoppages_df = cleaned_df.filter(
        (col("Arrival time") != "00:00:00") &
        (col("Departure Time") != "00:00:00")
    )

    # 3. Compute Stop Duration in minutes:
    # Convert HH:mm:ss to epoch seconds
    arr_sec = unix_timestamp(col("Arrival time"), "HH:mm:ss")
    dep_sec = unix_timestamp(col("Departure Time"), "HH:mm:ss")
    raw_diff = dep_sec - arr_sec

    # Handle midnight crossover: if departure is after midnight (e.g., arr 23:55, dep 00:05),
    # raw_diff is negative; adding 86400 seconds (24 hours) gives the true stop duration.
    stop_sec = when(raw_diff < 0, raw_diff + 86400).otherwise(raw_diff)
    stop_min = stop_sec / 60.0

    df_with_duration = stoppages_df.withColumn("stop_duration_minutes", stop_min)
    return df_with_duration


def perform_section_a_analysis(df):
    """
    Section A: Compute min, max, avg, 90th percentile stop duration per station,
    and identify top 5 stations with highest number of train stoppages.
    """
    print("\n" + "=" * 70)
    print("SECTION A: STOP DURATION STATISTICS")
    print("=" * 70)

    # Group by station and compute statistics
    station_stats = df.groupBy("Station Code", "Station Name").agg(
        count("Train No").alias("stoppage_count"),
        min("stop_duration_minutes").alias("min_stop_duration"),
        max("stop_duration_minutes").alias("max_stop_duration"),
        round(avg("stop_duration_minutes"), 2).alias("avg_stop_duration"),
        round(expr("percentile_approx(stop_duration_minutes, 0.90)"), 2).alias("p90_stop_duration"),
        round(sum("stop_duration_minutes"), 2).alias("total_stop_duration")
    )

    # Top 5 stations with highest number of train stoppages
    top5_stations = station_stats.orderBy(col("stoppage_count").desc()).limit(5)

    print("\nTop 5 Stations by Number of Train Stoppages:")
    top5_stations.select(
        "Station Code", "Station Name", "stoppage_count",
        "min_stop_duration", "max_stop_duration", "avg_stop_duration", "p90_stop_duration"
    ).show(truncate=False)

    return station_stats, top5_stations


def perform_section_b_analysis(df, station_stats, top5_stations):
    """
    Section B: Station Load Analysis
    - Stations where max stop duration > 3 * average stop duration
    - 2-hour sliding window analysis for one of the top 5 stations
    """
    print("\n" + "=" * 70)
    print("SECTION B: STATION LOAD ANALYSIS")
    print("=" * 70)

    # 1. Total stop duration & Average stop duration
    print("\nSample Station Load Summary (Total & Average Stop Duration):")
    station_stats.select(
        "Station Code", "Station Name", "total_stop_duration", "avg_stop_duration"
    ).show(5, truncate=False)

    # 2. Identify stations where: max stop duration > 3 * average stop duration
    stations_3x = station_stats.filter(col("max_stop_duration") > (3 * col("avg_stop_duration")))
    total_3x_count = stations_3x.count()
    print(f"\nStations where Max Stop Duration > 3 * Average Stop Duration (Total: {total_3x_count}):")
    stations_3x.select(
        "Station Code", "Station Name", "min_stop_duration", "max_stop_duration", "avg_stop_duration"
    ).show(10, truncate=False)

    # 3. Maximum number of trains arriving within any 2-hour sliding window for top station #1
    top_station_row = top5_stations.first()
    top_station_code = top_station_row["Station Code"]
    top_station_name = top_station_row["Station Name"]

    print(f"\nAnalyzing 2-hour Sliding Window for Top Station: {top_station_code} ({top_station_name})")

    # Filter records for this station and compute arrival time in epoch seconds
    stn_df = df.filter(col("Station Code") == top_station_code) \
               .withColumn("arr_sec", unix_timestamp(col("Arrival time"), "HH:mm:ss"))

    # To handle 24-hour circular wrap-around (e.g. window starting at 23:00 to 01:00),
    # create a 2nd day projection and union
    day1_df = stn_df.select("Train No", "arr_sec")
    day2_df = stn_df.select("Train No", (col("arr_sec") + lit(86400)).alias("arr_sec"))
    circular_df = day1_df.union(day2_df)

    # Range frame: [arr_sec, arr_sec + 7200 seconds (2 hours)]
    window_2hr = Window.orderBy("arr_sec").rangeBetween(0, 7200)

    windowed_arrivals = circular_df.withColumn(
        "trains_in_2hr_window",
        count("Train No").over(window_2hr)
    )

    # Only evaluate windows starting within the 24-hour cycle
    max_trains = windowed_arrivals.filter(col("arr_sec") < 86400) \
                                  .agg(max("trains_in_2hr_window")) \
                                  .collect()[0][0]

    print(f"Maximum number of trains arriving within any 2-hour sliding window at {top_station_code}: {max_trains}")


def perform_section_c_export(station_stats, output_path):
    """
    Section C: Store the aggregated results as a CSV file in GCS.
    """
    print("\n" + "=" * 70)
    print(f"SECTION C: EXPORTING AGGREGATED RESULTS TO: {output_path}")
    print("=" * 70)

    # Export a single consolidated CSV file with headers
    station_stats.coalesce(1) \
                 .write \
                 .mode("overwrite") \
                 .option("header", "true") \
                 .csv(output_path)

    print("Aggregated CSV export completed successfully.")


def main():
    parser = argparse.ArgumentParser(description="OPPE 1 PySpark Analysis")
    parser.add_argument("--input", default="Train_details_22122017.csv", help="Input dataset path (local or gs://)")
    parser.add_argument("--output", default="output/station_summary", help="Output directory path (local or gs://)")
    args, unknown = parser.parse_known_args()

    # If positional arguments are passed (standard for Dataproc submit)
    if len(unknown) >= 2:
        input_path = unknown[0]
        output_path = unknown[1]
    elif len(unknown) == 1:
        input_path = unknown[0]
        output_path = args.output
    else:
        input_path = args.input
        output_path = args.output

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # Pipeline Execution
    df = clean_and_prepare_data(spark, input_path)
    station_stats, top5_stations = perform_section_a_analysis(df)
    perform_section_b_analysis(df, station_stats, top5_stations)
    perform_section_c_export(station_stats, output_path)

    spark.stop()
    print("\nPySpark Analysis Finished Successfully.")


if __name__ == "__main__":
    main()
