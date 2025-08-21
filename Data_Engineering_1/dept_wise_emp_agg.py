from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import expr, to_date, when, col, desc, row_number


def create_employees_dataframe(spark):
    """
    Creates a PySpark DataFrame containing customer details.
    Args:
        spark (SparkSession): The current SparkSession.
    Returns:
        DataFrame: Customer DataFrame with pre-defined schema.
    """
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

    emp_df = spark.createDataFrame(data=data, schema=columns)

    # Convert columns to IntegerType
    emp_df = emp_df.withColumn("Date", col("Date").cast(IntegerType())) \
           .withColumn("Month", col("Month").cast(IntegerType())) \
           .withColumn("Year", col("Year").cast(IntegerType()))

    # Calculate Actual_Birth_Year
    emp_df = emp_df.withColumn(
        "Actual_Birth_Year",
        when(col("Year") < 21, col("Year") + 2000)
        .when(col("Year") < 100, col("Year") + 1900)
        .otherwise(col("Year"))
    )

    # Create DOB column
    emp_df = emp_df.withColumn(
        "DOB",
        to_date(expr("concat(Date, '/', Month, '/', Actual_Birth_Year)"), "d/M/y")
    )

    return emp_df

if __name__ == "__main__":
  print("=== Package: Data_Engineering_1 | Script: dept_wise_emp_agg ===")

  # Initialize Spark session
  spark = SparkSession.builder \
      .appName("dept_wise_emp_agg") \
      .master("local[3]") \
      .getOrCreate()

  # Suppress unnecessary Spark logging
  spark.sparkContext.setLogLevel("ERROR")

  # Create and display DataFrame
  emp_df = create_employees_dataframe(spark)
  print("DataFrame:")

  emp_df.printSchema()
  emp_df.select("*").show(n = 10, truncate=False)

  spark.stop()