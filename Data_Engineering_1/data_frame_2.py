from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col


def create_customer_dataframe(spark):
    """
    Creates a PySpark DataFrame containing customer details.
    """
    schema = StructType([
        StructField("ID", IntegerType(), True),
        StructField("F_Name", StringType(), True),
        StructField("L_Name", StringType(), True),
        StructField("Origin", StringType(), True),
        StructField("Occupation", StringType(), True)
    ])

    data = [
        (100, "Monica", "Bellucci", "Italy", "Modeling"),
        (110, "Kate", "Upton", "USA", "Modeling"),
        (120, "Kate", "Winslet", "USA", "Acting"),
        (130, "Pierce ", "Brosnan", "USA", "Acting"),
        (140, "Tom", "Cruise", "USA", "Acting"),
        (150, "Chris", "Patt", "USA", "Acting"),
        (160, "Daniel", "Craig", "UK", "Acting"),
        (170, "Blake", "Lively", "USA", "Modeling"),
        (180, "Tom", "Hardy", "USA", "Acting")
        ]

    columns = ["ID", "F_Name", "L_Name", "Origin", "Occupation"]

    df = spark.createDataFrame(data, columns)
    return df

if __name__ == "__main__":
    print("=== Package: Data_Engineering_1 | Script: Data_Frame_2 ===")

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Data_Frame_2") \
        .master("local[3]") \
        .getOrCreate()

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")

    # Create and display DataFrame
    customer_df = create_customer_dataframe(spark)
    print("Customer DataFrame:")
    # customer_df.show(truncate=False)
    customer_df.select("*").orderBy(col("ID").asc()).show(truncate=False)

    # Stop Spark session
    spark.stop()