from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : Data_Engineering_1, Script : Date Format 2")
    spark = SparkSession.builder \
        .appName("Date_Format_1") \
        .master("local[3]") \
        .getOrCreate()

    # logger = Log4J()
    # logger.info("Starting Data_Format_1")

    # define schema for manually created data frame
    def_schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("EventDate", DateType(), True)]
      )
    
    # define the columns
    columns = ["ID","EventDate"]
    
    # define the data
    data = [(100, "01-01-2020"), (110, "02-02-2020"),(120, "03-03-2020")]

    df = spark.createDataFrame(data=data, schema = columns)
    # logger.info("Ending Data_Format_1")
    print("Let's print data type")
    df.printSchema()
    df.show()