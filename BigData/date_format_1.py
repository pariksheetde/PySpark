from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : BigData, Script : Date Format 1")

spark = SparkSession.builder.appName("Date Format using DataFrame 1").master("local[3]").getOrCreate()

def_schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("EventDate", StringType(), True)]
  )
columns = ["ID","EventDate"]

data = [(100,"01/01/2020"), (110,"02/02/2020"),(120,"03/03/2020")]

df = spark.createDataFrame(data=data, schema = columns)
df.show()

spark.stop()