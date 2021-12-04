import pyspark.sql.functions as F

METERS_PER_FOOT = 0.3048
FEET_PER_MILE = 5280
EARTH_RADIUS_IN_METERS = 6371e3
METERS_PER_MILE = METERS_PER_FOOT * FEET_PER_MILE


def compute_distance(_spark, dataframe):
    df = dataframe.withColumn("a", (

            F.pow(F.sin(F.radians(F.col("end_station_latitude") - F.col("start_station_latitude")) / 2), 2) +

            F.cos(F.radians(F.col("start_station_latitude"))) * F.cos(F.radians(F.col("end_station_latitude"))) *

            F.pow(F.sin(F.radians(F.col("end_station_longitude") - F.col("start_station_longitude")) / 2), 2)

    )).withColumn("distance", F.atan2(F.sqrt(F.col("a")), F.sqrt(-F.col("a") + 1)) * 12742000) \
        .sort('tripduration', ascending=True)
    # df = df.drop("a")
    return df


def run(spark, input_dataset_path, transformed_dataset_path):
    input_dataset = spark.read.parquet(input_dataset_path)
    input_dataset.show()

    dataset_with_distances = compute_distance(spark, input_dataset)
    dataset_with_distances.show()

    dataset_with_distances.write.parquet(transformed_dataset_path, mode='append')
