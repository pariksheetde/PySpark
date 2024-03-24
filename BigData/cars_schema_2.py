from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : BigData, Script : Cars Schema 2")

spark = SparkSession.builder.appName("German Cars using DataFrame 2").master("local[3]").getOrCreate()

data = [
    (100, "Audi", "Audi Q4"),
    (110, "BMW", "BMW X4 Roadstar"),
    (120, "Mercedes", "")
  ]

# schema = StructType([
#     StructField("ID",IntegerType(),True),
#     StructField("Model",StringType(),True),
#     StructField("Brand", StringType(), True)]
#   )

df = spark.createDataFrame(data, schema = "ID INT, Name STRING, Model STRING")
df.printSchema()
df.show()