from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Department Employees Analysis 2")

spark = SparkSession.builder.appName("Dept_Emp_Analysis_2").master("local[3]").getOrCreate()

dept_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("inferSchema", "true") \
    .option("nullValue", "NA") \
    .option("sep", ",") \
    .option("compression", "snappy") \
    .option("dateFormat", "dd/MM/yyy") \
    .load("D:/DataSet/DataSet/SparkDataSet/departments.csv")

emp_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("inferSchema", "true") \
    .option("nullValue", "NA") \
    .option("sep", ",") \
    .option("compression", "snappy") \
    .option("dateFormat", "dd/MM/yyy") \
    .load("D:/DataSet/DataSet/SparkDataSet/employees.csv")

cln_emp_df = emp_df.selectExpr("EMPLOYEE_ID as EmpID", "FIRST_NAME as F_Name", "LAST_NAME as L_Name", "EMAIL as Email_ID",
      "PHONE_NUMBER as Contact", "HIRE_DATE as Joining_Date", "JOB_ID as Job_ID", "SALARY as Salary", "COMMISSION_PCT as Commission",
      "MANAGER_ID as Manager_ID", "DEPARTMENT_ID as DeptID")

# cln_emp_df.show(100)

dept_emp_df = dept_df.join(cln_emp_df, dept_df.DEPT_ID == cln_emp_df.DeptID, "inner") \
    .select(cln_emp_df["EmpID"],  cln_emp_df["DeptID"], dept_df["DEPT_NAME"], cln_emp_df["F_Name"], cln_emp_df["L_Name"],
            cln_emp_df["Email_ID"], cln_emp_df["Joining_Date"], cln_emp_df["Salary"], cln_emp_df["Contact"],
            cln_emp_df["Commission"]) \
    .fillna(value=0, subset=["Commission"]) \
    # .where(col("Commission") != 0.0)

dept_emp_df.show(100, False)
print(f"Number of records {dept_emp_df.count()}")