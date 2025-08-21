from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col


def create_customer_dataframe(spark):
    """
    Creates a PySpark DataFrame containing customer details.
    Args:
        spark (SparkSession): The current SparkSession.
    Returns:
        DataFrame: Customer DataFrame with pre-defined schema.
    """
    schema = StructType([
        StructField("F_Name", StringType(), True),
        StructField("L_Name", StringType(), True),
        StructField("Salary", IntegerType(), True),
        StructField("Nationality", StringType(), True),
    ])

    data = [
        ["Monica", "Bellucci", 2_500_000, "Italy"],
        ["Kate", "Upton", 2_250_000, "USA"],
        ["Gigi", "Hadid", None, "USA"],
        ["Barbara", "Palvin", 5_414_782, "USA"],
        ["Blake", "Lively", 2_575_000, "USA"],
        ["Jennifer", "Lopez", 3_58_000, "USA"],
    ]

    df = spark.createDataFrame(data, schema)
    return df

if __name__ == "__main__":
    print("=== Package: Data_Engineering_1 | Script: Data_Frame_1 ===")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Data_Frame_1") \
        .master("local[3]") \
        .getOrCreate()

    # Create and display DataFrame
    customer_df = create_customer_dataframe(spark)
    print("Customer DataFrame:")
    # customer_df.show(truncate=False)
    customer_df.select("F_Name", "L_Name", "Salary", "Nationality").orderBy(col("Salary").desc()).show(truncate=False)

    # Stop Spark session
    spark.stop()
