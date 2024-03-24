from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : Data_Engineering_1, Script : Date Format 4")

spark = SparkSession.builder.appName("Date_Format_4").master("local[3]").getOrCreate()

def_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("EventDate", StringType(), True)]
  )
columns = ["Name","Date", "Month", "Year"]

data = [("Monica", "12", "1", "2002"),
        ("Kate", "14", "10", "81"),
        ("Peter", "25", "05", "63"),
        ("Pamela", "16", "12", "06"),
        ("Kylie", "19", "9", "85")
        ]

df = spark.createDataFrame(data=data, schema = columns)

res_df = df.withColumn("id", monotonically_increasing_id()) \
    .withColumn("Date", col("Date").cast(IntegerType())) \
    .withColumn("Month", col("Month").cast(IntegerType())) \
    .withColumn("Year", col("Year").cast(IntegerType())) \
    .select(col("id"), col("Name"), col("Date"), col("Month"), col("Year"),
            when(col("Year") < 21, col("Year").cast("Int") + 2000) \
            .when(col("Year") < 100, col("Year").cast("Int") + 1900) \
            .otherwise(col("Year")).alias("Actual_Birth_Year")) \
    .withColumn("DOB", to_date(expr("concat(Date, '/', Month, '/', Actual_Birth_Year)"), "d/M/y")) \
    .drop("Date", "Month", "Year") \
    .sort(desc(col("DOB")))

res_df.printSchema()
res_df.show(10, False)

spark.stop()