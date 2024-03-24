from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    print("Package : Bank, Script : Flight_Data_Analysis_3")

spark = SparkSession.builder.appName("Flight_Data_Analysis_3").master("local[3]").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 100)
spark.conf.set("spark.default.parallelism", 100)

flight_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("D:/DataSet/DataSet/SparkDataSet/flight.csv")

flight_part = flight_df.repartition(10)
print(f"Number of Partition : {flight_part.rdd.getNumPartitions()}")
SQL_tab = flight_part.createOrReplaceTempView("flight_analysis")

SQL_qry = spark.sql("select * from flight_analysis")

SQL_qry.groupBy(spark_partition_id()).count() \
    .orderBy("SPARK_PARTITION_ID()") \
    .show()

SQL_qry.write.format('csv').mode('overwrite').save("D:/DataSet/OutputDataset/flight_data")

spark.stop()
