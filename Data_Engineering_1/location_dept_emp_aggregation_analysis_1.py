from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_1, Script : location_dept_emp_aggregation_analysis_1")

spark = SparkSession.builder.appName("IPL Analysis 1").master("local[3]").getOrCreate()


locations_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Coronavirus/locations.csv"))

# locations_df.show(10, truncate = False)

departments_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Coronavirus/departments.csv"))

# departments_df.show(10, truncate = False)

employees_df = (spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Coronavirus/employees.csv"))

employees_df.select("*") \
    .withColumnRenamed("DEPARTMENT_ID", "EMP_DEPT_EMP_ID")

emp_agg_salary_df = employees_df.groupBy("DEPARTMENT_ID") \
    .agg(
    max("SALARY").alias("MAX_SALARY")
)
# emp_agg_salary_df.show()

dept_emp_join_df = (departments_df.join(emp_agg_salary_df, departments_df.DEPT_ID == emp_agg_salary_df.DEPARTMENT_ID) \
    .select(departments_df["DEPT_ID"], departments_df["LOC_ID"], departments_df["DEPT_NAME"], emp_agg_salary_df["MAX_SALARY"]))
# dept_emp_join_df.show()

dept_emp_emp_self_join_df = (employees_df.join(dept_emp_join_df, employees_df.DEPARTMENT_ID == dept_emp_join_df.DEPT_ID) \
                             .select("EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME", "DEPT_ID", "DEPT_NAME", "LOC_ID", "SALARY", "MAX_SALARY")) \
    .where("SALARY = MAX_SALARY")
# dept_emp_emp_self_join_df.show()

process_df = dept_emp_emp_self_join_df.join(locations_df, dept_emp_emp_self_join_df.LOC_ID == locations_df.LOCATION_ID) \
    .select("EMPLOYEE_ID", "FIRST_NAME", "LAST_NAME", "DEPT_ID", "DEPT_NAME", "LOC_ID", "SALARY", "CITY", "MAX_SALARY")
process_df.show()
spark.stop()