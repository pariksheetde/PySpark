from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    print("Package : Bank, Script : Flight_Data_Analysis_4")

spark = SparkSession.builder.appName("Flight_Data_Analysis_4").master("local[3]").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 100)
spark.conf.set("spark.default.parallelism", 100)

flight_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("D:/DataSet/DataSet/SparkDataSet/flight.csv")

flight_part = flight_df.repartition(10)

# create temp table and query temp view
SQL_tab = flight_part.createOrReplaceTempView("flight_analysis")
SQL_qry = spark.sql("select * from flight_analysis")

SQL_qry.groupBy(spark_partition_id()).count().show()

SQL_qry.write \
      .format("csv") \
      .partitionBy("Source", "destination") \
      .mode('overwrite') \
      .option("path", "D:/DataSet/OutputDataset/flight_data/") \
      .save()

spark.stop()