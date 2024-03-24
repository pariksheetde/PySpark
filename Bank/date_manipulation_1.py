from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    print("Package : Bank, Script : Date Manipulation 1")

spark = SparkSession.builder.appName("Date Manipulation 1").master("local[3]").getOrCreate()


def date_conversion(df=DataFrame, fmt=StringType, fld=StringType):
    return df.withColumn(fld, to_date(col(fld), fmt))


print(f"Before conversion")

data = [(100, "01/01/2020"),
        (110, "02/02/2020"),
        (120, "03/03/2020")]

cols = ["ID", "EventDate"]
deptDF = spark.createDataFrame(data = data, schema = cols)
deptDF.printSchema()
deptDF.show()


print(f"After conversion")

func = date_conversion(deptDF, "M/d/y", "EventDate")
func.printSchema()
func.show()


spark.stop()
