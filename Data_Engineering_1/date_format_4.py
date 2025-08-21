from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import expr, to_date, when, col, desc, row_number


from pyspark.sql.window import Window

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
    ])

    columns = ["Name", "Date", "Month", "Year"]

    data = [("Monica", "12", "1", "2002"),
            ("Kate", "14", "10", "81"),
            ("Peter", "25", "05", "63"),
            ("Pamela", "16", "12", "06"),
            ("Kylie", "19", "9", "85")
            ]

    df = spark.createDataFrame(data=data, schema=columns)

    # Convert columns to IntegerType
    df = df.withColumn("Date", col("Date").cast(IntegerType())) \
           .withColumn("Month", col("Month").cast(IntegerType())) \
           .withColumn("Year", col("Year").cast(IntegerType()))

    # Calculate Actual_Birth_Year
    df = df.withColumn(
        "Actual_Birth_Year",
        when(col("Year") < 21, col("Year") + 2000)
        .when(col("Year") < 100, col("Year") + 1900)
        .otherwise(col("Year"))
    )

    # Create DOB column
    df = df.withColumn(
        "DOB",
        to_date(expr("concat(Date, '/', Month, '/', Actual_Birth_Year)"), "d/M/y")
    )

    # Generate sequence using row_number()
    window_spec = Window.orderBy(col("DOB").desc())
    df = df.withColumn("id", row_number().over(window_spec))

    # Select and sort
    res_df = df.select("id", "Name", "DOB", "Actual_Birth_Year") \
               .sort(desc(col("DOB")))

    return res_df

if __name__ == "__main__":
    print("=== Package: Data_Engineering_1 | Script: Date_Format_4 ===")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Date_Format_4") \
        .master("local[3]") \
        .getOrCreate()

    # Create and display DataFrame
    res_df = create_dataframe(spark)
    print("DataFrame:")

    res_df.printSchema()
    res_df.select("*").show(n = 10, truncate=False)
        
    # Stop Spark session
    spark.stop()