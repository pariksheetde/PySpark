from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

if __name__ == "__main__":
    print("Package : BigData, Script : Department-Employee Analysis 1")

spark = SparkSession.builder.appName("Department-Employee Analysis").master("local[3]").getOrCreate()

dept_schema = StructType([
    StructField("Dept_ID", IntegerType(), True),
    StructField("DEPT_NAME", StringType(), True),
    StructField("LOC_ID", StringType(), True)]
)

emp_schema = StructType([
    StructField("EMPLOYEE_ID", IntegerType()),
    StructField("FIRST_NAME", StringType()),
    StructField("LAST_NAME", StringType()),
    StructField("EMAIL", StringType()),
    StructField("PHONE_NUMBER", StringType()),
    StructField("HIRE_DATE", StringType()),
    StructField("JOB_ID", StringType()),
    StructField("SALARY", IntegerType()),
    StructField("COMMISSION_PCT", FloatType()),
    StructField("MANAGER_ID", IntegerType()),
    StructField("DEPARTMENT_ID", IntegerType())]
)

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
dept_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(dept_schema) \
    .option("dateFormat", "dd-MMM-yy") \
    .option("mode", "PERMISSIVE") \
    .option("sep", ",") \
    .option("nullValue", "null") \
    .option("compression", "snappy") \
    .option("path", "D:/DataSet/DataSet/SparkDataSet/departments.csv") \
    .load()
# dept_df.show()

emp_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(emp_schema) \
    .option("dateFormat", "dd-MM-yyyy") \
    .option("mode", "PERMISSIVE") \
    .option("sep", ",") \
    .option("nullValue", "") \
    .option("compression", "snappy") \
    .option("path", "D:/DataSet/DataSet/SparkDataSet/employees.csv") \
    .load()
# emp_df.show()

dept_emp_join = dept_df.join(emp_df.withColumnRenamed("Manager_ID", "Man_ID"), dept_df.Dept_ID == emp_df.DEPARTMENT_ID,
                             "inner").selectExpr("EMPLOYEE_ID as emp_id", "FIRST_NAME as f_name", "LAST_NAME as l_name", "MAN_ID as manager_id",
                "DEPT_NAME as dept_name", "HIRE_DATE as joining_dt")

dept_emp_join.printSchema()
dept_emp_join.show(10, False)

spark.stop()