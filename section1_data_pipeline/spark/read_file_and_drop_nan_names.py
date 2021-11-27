import sys
from pyspark.sql import SparkSession

# create spark session
spark = SparkSession.builder \
    .getOrCreate()

def read_file_and_drop_nan_names(data_path: str):
    # read csv file
    df = spark.read.format("csv").option("header", "true").load(
        data_path + "/raw_data/dataset2.csv")
    # drop names with nan
    df = df.na.drop(subset=["name"])
    df.coalesce(1).write.format('com.databricks.spark.csv').save(
        data_path + "/raw_data/dataset_v1.csv", header='true')
    spark.stop()


def main():
    file_path = sys.argv[1]
    read_file_and_drop_nan_names(file_path)


if __name__ == '__main__':
    main()
