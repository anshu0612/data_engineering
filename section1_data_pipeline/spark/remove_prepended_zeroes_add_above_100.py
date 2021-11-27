import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import pyspark.sql.functions as F

# create spark session
spark = SparkSession.builder \
    .getOrCreate()


def remove_prepended_zeroes_add_above_100(data_path: str):
    # read processed csv file with dropped nan names
    df = spark.read.format("csv").option("header", "true").load(
        data_path + "/raw_data/dataset_v1.csv")

    # casting price as string
    df = df.withColumn("price", df["price"].cast(StringType()))
    # remove trailing zeros
    df = df.withColumn('price', F.regexp_replace('price', r'^[0]*', ''))
    # add above_100
    df = df.withColumn('above_100', F.when(
        F.col('price') > 100, True).otherwise(False))

    # save only the price amd above_100 columns
    df = df.select("price", "above_100")
    df.coalesce(1).write.format('com.databricks.spark.csv').save(
        data_path + "/raw_data/dataset_price.csv", header='true')
    spark.stop()


def main():
    file_path = sys.argv[1]
    remove_prepended_zeroes_add_above_100(file_path)


if __name__ == '__main__':
    main()
