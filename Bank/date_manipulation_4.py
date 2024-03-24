from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType

if __name__ == "__main__":
    print("Package : Bank, Script : Date Manipulation 4")

spark = SparkSession.builder.appName("Date Manipulation 4").master("local[3]").getOrCreate()

columns = Row("Date", "Month", "Year")
data = [Row("14", "09", "2000"), Row("28", "02", "1999")]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show()

date_df = df.select(col("Date"), col("Month"), col("Year"),
                    expr("to_date(concat(Date, Month, Year),'ddMMyyyy') as Date_Of_Birth"))
date_df.printSchema()
date_df.show()

spark.stop()