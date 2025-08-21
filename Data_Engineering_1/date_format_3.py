from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col, monotonically_increasing_id, when


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

    df = spark.createDataFrame(data=data, schema = columns)

    res_df = df.withColumn("id", monotonically_increasing_id()) \
        .withColumn("Date", col("Date").cast(IntegerType())) \
        .withColumn("Month", col("Month").cast(IntegerType())) \
        .withColumn("Year", col("Year").cast(IntegerType())) \
        .select(col("id"), col("Name"), col("Date"), col("Month"), col("Year"),
                when(col("Year") < 21, col("Year").cast("Int") + 2000) \
                .when(col("Year") < 100, col("Year").cast("Int") + 1900) \
                .otherwise(col("Year")).alias("Actual_Birth_Year"))
    return res_df

if __name__ == "__main__":
    print("=== Package: Data_Engineering_1 | Script: Date_Format_3 ===")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Date_Format_3") \
        .master("local[3]") \
        .getOrCreate()

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")

    # Create and display DataFrame
    df = create_dataframe(spark)
    print("DataFrame:")

    df.printSchema()
    df.select("id","Name", "Date", "Month", "Year", "Actual_Birth_Year").show(df.count(), truncate=False)
    print("Total Records: ", df.count())
    # Stop Spark session
    spark.stop()