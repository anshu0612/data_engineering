import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# create spark session
spark = SparkSession.builder \
    .getOrCreate()

def merge_cols_and_save_file(data_path: str):
    # read processed csv file with dropped nan names
    df_name = spark.read.format("csv").option("header", "true").load(
        data_path + "/raw_data/dataset_name.csv")
    df_price = spark.read.format("csv").option("header", "true").load(
        data_path + "/raw_data/dataset_price.csv")

    # adding dumming id1 and id2 to merge the two dataframes
    df_name = df_name.withColumn("id1", F.monotonically_increasing_id())
    df_price = df_price.withColumn("id2", F.monotonically_increasing_id())

    # merge columns in df_name and df_price
    df_final = df_name.join(df_price, F.col(
        "id1") == F.col("id2"), "inner").drop("id1", "id2")

    # save processed dataset
    df_final.coalesce(1).write.format('com.databricks.spark.csv').save(
        data_path + "/processed_data/dataset.csv", header='true')
    spark.stop()

def main():
    file_path = sys.argv[1]
    merge_cols_and_save_file(file_path)


if __name__ == '__main__':
    main()
