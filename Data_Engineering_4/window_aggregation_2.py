from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: Employee Window Aggregation using DataFrame 2")

spark = SparkSession.builder.appName("Employee Window Aggregation using DataFrame 2").master("local[3]").getOrCreate()

data = [
    ("Sales", 1, 5000),
    ("Personnel", 2, 3900),
    ("Sales", 3, 5500),
    ("Sales", 4, 7500),
    ("Personnel", 5, 3500),
    ("Developer", 7, 4200),
    ("Developer", 8, 6000),
    ("Developer", 9, 4500),
    ("Developer", 10, 5200),
    ("Developer", 11, 5200)]

columns = ["Dept","Emp_ID", "Salary"]

df = spark.createDataFrame(data=data, schema = columns)

df.printSchema()

sal_dept = df.select("Emp_ID", "Dept", "Salary") \
      .withColumn("Max_Salary", min("Salary").over(Window.partitionBy("Dept").orderBy(asc("Dept"), desc("Salary")))) \
      .withColumn("Rank", dense_rank().over(Window.partitionBy("Dept").orderBy(desc("Max_Salary"))))

sal_dept.show(truncate=False)
spark.stop()