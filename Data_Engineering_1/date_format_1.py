from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import col


def create_dataframe(spark):
    """
    Creates a PySpark DataFrame containing dataframe details.
    """
    # define schema for manually created data frame
    schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("EventDate", DateType(), True)]
      )

    # define the data
    data = [(100, "01-01-2020"), (110, "02-02-2020"),(120, "03-03-2020")]

    # define the columns
    columns = ["ID","EventDate"]

    df = spark.createDataFrame(data, columns)
    return df
    
if __name__ == "__main__":
  print("=== Package: Data_Engineering_1 | Script: Date_Format_1 ===")

  # Initialize Spark session
  spark = SparkSession.builder \
      .appName("Data_Frame_2") \
      .master("local[3]") \
      .getOrCreate()

  # Suppress unnecessary Spark logging
  spark.sparkContext.setLogLevel("ERROR")

  # Create and display DataFrame
  df = create_dataframe(spark)
  print("DataFrame:")
  
  df.select("*").show(truncate=False)
  df.printSchema()
  # Stop Spark session
  spark.stop()