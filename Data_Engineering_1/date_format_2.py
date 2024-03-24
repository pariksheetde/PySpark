from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : Data_Engineering_1, Script : Date Format 2")

spark = SparkSession.builder.appName("Date_Format_2").master("local[3]").getOrCreate()

def_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("EventDate", StringType(), True)]
  )
columns = ["Name","Date", "Month", "Year"]

data = [("Monica", "12", "1", "2002"),
        ("Kate", "14", "9", "2010"),
        ("Peter", "31", "3", "05"),
        ("Pamela", "15", "6", "10")]

df = spark.createDataFrame(data=data, schema = columns)
df.printSchema()
df.show()

res_df = df.withColumn("id", monotonically_increasing_id()) \
    .withColumn("Date", col("Date").cast(IntegerType())) \
    .withColumn("Month", col("Month").cast(IntegerType())) \
    .withColumn("Year", col("Year").cast(IntegerType()))

res_df.printSchema()
res_df.show(10, False)

spark.stop()