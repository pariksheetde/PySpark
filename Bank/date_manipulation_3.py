from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    print("Package : Bank, Script : Date Manipulation 3")

spark = SparkSession.builder.appName("Date Manipulation 3").master("local[3]").getOrCreate()

data = [Row("2019-07-01"), Row("2019-06-24"), Row("2019-11-16"), Row("2019-11-24")]
schema = StructType([StructField('DOJ', StringType(), True)])

df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()

dt_conversion = df.select("DOJ").withColumn("Date_of_Joining", to_date("DOJ", "yyyy-MM-dd"))

year_df = dt_conversion.select(dayofmonth("DOJ").alias("Day"), month("DOJ").alias("Month"), year("DOJ").alias("Year"))
year_df.printSchema()
year_df.show()

spark.stop()