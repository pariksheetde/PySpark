from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_2, Script : Departments Analysis")

spark = SparkSession.builder.appName("Departments_Analysis_1").master("local[3]").getOrCreate()

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

sel_dept = dept_df.selectExpr("dept_id as DeptID", "dept_name as Dept_Nm", "loc_id as LocID")

sel_dept.printSchema()
sel_dept.show(10, False)

spark.stop()