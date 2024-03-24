from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    print("Package : Bank, Script : Date Manipulation 2")

spark = SparkSession.builder.appName("Date Manipulation 2").master("local[3]").getOrCreate()


def date_conversion(df=DataFrame, fmt=StringType, fld=StringType):
    return df.withColumn(fld, to_date(col(fld), fmt))


data = [Row(100, "01/01/2020"), Row(110, "02/02/2020"), (120, "03/30/2021")]

schema = StructType([StructField('ID', StringType(), True),
                     StructField('Event_DT', StringType(), True)])

print(f"Before conversion")
df = spark.createDataFrame(data, schema)
df.printSchema()
df.show()

print(f"After Conversion")
func = date_conversion(df, "M/d/y", "Event_DT")
func.printSchema()
func.show()

spark.stop()