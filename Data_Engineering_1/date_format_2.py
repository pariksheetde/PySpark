from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col


def create_dataframe(spark):
    """
    Creates a PySpark DataFrame containing dataframe details.
    """
    # define schema for manually created data frame
    schema = StructType([
        StructField("Name", StringType(), True),
        StructField("Date", IntegerType(), True),
        StructField("Month", IntegerType(), True),
        StructField("Year", IntegerType(), True)
        ]
      )

    # define the data
    data = [("Monica", "12", "1", "2002"),
        ("Kate", "14", "9", "2010"),
        ("Peter", "31", "3", "2005"),
        ("Pamela", "15", "6", "2010")]

    # define the columns
    columns = ["Name","Date", "Month", "Year"]

    df = spark.createDataFrame(data, columns)
    df = df.withColumn("Date", col("Date").cast(IntegerType())) \
           .withColumn("Month", col("Month").cast(IntegerType())) \
           .withColumn("Year", col("Year").cast(IntegerType()))
    return df

if __name__ == "__main__":
    print("=== Package: Data_Engineering_1 | Script: Date_Format_2 ===")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Date_Format_2") \
        .master("local[3]") \
        .getOrCreate()

    # Create and display DataFrame
    df = create_dataframe(spark)
    print("DataFrame:")

    df.printSchema()
    df.select("Name", "Date", "Month", "Year").show(n = 10, truncate=False)
        
    # Stop Spark session
    spark.stop()