from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType


if __name__ == "__main__":
    print("People's JSON File")

spark = SparkSession.builder.appName("Indian Premier League DataSet API").master("local[3]").getOrCreate()

spark.conf.set("spark.sql.shuffle.partitions", 100)
spark.conf.set("spark.default.parallelism", 100)

people_df  = (spark.read
    .json("D:/DataSet/DataSet/SparkDataSet/people.json"))

people_df = people_df.selectExpr("*")
people_df.show(20, truncate = False)
people_df.printSchema()

spark.stop()

