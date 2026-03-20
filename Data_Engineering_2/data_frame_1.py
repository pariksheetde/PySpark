from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

def process_customers_df(spark):
    print("=== Package: Data_Engineering_2 | Script: Data_Frame_1 ===")

    schema = StructType([
        StructField("F_Name", StringType(), True),
        StructField("L_Name", StringType(), True),
        StructField("Salary", IntegerType(), True)
    ])

    data = [["Monica", "Bellucci", 2500000], ["Kate", "Upton", 2250000], ["Kate", "Beckinsale", 1700000],
            ["Audry", "Hepburn", 1750000],["Jennifer", "Lawrence", 1957000], ["Carmen", "Electra", 1550000],
            ["Jennifer", "Garner", 3225000], ["Michelle", "Obama", 1400500], ["Kirsten", "Steward", 2150000],
            ["Sophia", "Turner", 2945000]
            ]

    cust_df = spark.createDataFrame(data, schema)
    return cust_df

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("Data_Frame_1") \
        .master("local[3]") \
        .getOrCreate()

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")

    filtered_cust_df = process_customers_df(spark)
    filtered_cust_df.filter(col("Salary") > 2000000).orderBy(col("Salary").desc())
    
    filtered_cust_df.show(filtered_cust_df.count(), truncate=False)
    print(f"Total Records Processed: {filtered_cust_df.count()}")

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")

    # Stop Spark session
    spark.stop()