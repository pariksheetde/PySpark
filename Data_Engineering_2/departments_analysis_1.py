from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

def generate_department_analysis(spark):
    
    dept_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True),
    StructField("loc_id", IntegerType(), True)
    ])

    dept_df = spark.read \
    .format("csv") \
    .schema(dept_schema) \
    .option("header", "true") \
    .load("D:/DataSet/DataSet/SparkDataSet/departments.csv")

    select_required_columns_df = dept_df.selectExpr("dept_id as DeptID", "dept_name as DeptName", "loc_id as LocID")

    return select_required_columns_df

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Departments Analysis 1").master("local[3]").getOrCreate()

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")

    result_df = generate_department_analysis(spark)
    result_df.show(100, False)
    print(f"Total Records Processed: {result_df.count()}")

    # Suppress unnecessary Spark logging
    spark.sparkContext.setLogLevel("ERROR")
    
    # Stop Spark session
    spark.stop()