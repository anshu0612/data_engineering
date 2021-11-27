import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# create spark session
spark = SparkSession.builder \
    .getOrCreate()


def split_names(data_path: str):
    # read processed csv file with dropped nan names
    df = spark.read.format("csv").option("header", "true").load(
        data_path + "/raw_data/dataset_v1.csv")
    # split name into first_name, last_name
    split_name_col = F.split(df['name'], ' ')
    df = df.withColumn('first_name', split_name_col.getItem(0))
    df = df.withColumn('last_name', split_name_col.getItem(1))

    # save only the first_name amd last_name columns
    df = df.select("first_name", "last_name")

    df.coalesce(1).write.format('com.databricks.spark.csv').save(
        data_path + "/raw_data/dataset_name.csv", header='true')
    spark.stop()


def main():
    file_path = sys.argv[1]
    split_names(file_path)


if __name__ == '__main__':
    main()
