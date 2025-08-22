from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


def read_from_locations_csv(spark):
    """
    Reads data from a CSV file into a PySpark DataFrame.
    Args:
        spark (SparkSession): The current SparkSession.
    Returns:
        DataFrame: DataFrame containing the locations data from the CSV file.
    """
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
    return locations_df


def read_from_departments_csv(spark):
    """
    Reads data from a CSV file into a PySpark DataFrame.
    Args:
        spark (SparkSession): The current SparkSession.
    Returns:
        DataFrame: DataFrame containing the departments data from the CSV file.
    """
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
    return departments_df

def read_from_employees_csv(spark):
    """
    Reads data from a CSV file into a PySpark DataFrame.
    Args:
        spark (SparkSession): The current SparkSession.
    Returns:
        DataFrame: DataFrame containing the employees data from the CSV file.
    """
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
    

    employees_modified_df = employees_df.select("*") \
    .withColumnRenamed("EMPLOYEE_ID", "Emp_ID") \
    .withColumnRenamed("FIRST_NAME", "First_Name") \
    .withColumnRenamed("LAST_NAME", "Last_Name") \
    .withColumnRenamed("EMAIL", "Email") \
    .withColumnRenamed("PHONE_NUMBER", "Phone_Number") \
    .withColumnRenamed("HIRE_DATE", "Hire_Date") \
    .withColumnRenamed("JOB_ID", "Job_ID") \
    .withColumnRenamed("SALARY", "Salary") \
    .withColumnRenamed("COMMISSION_PCT", "Commission_Pct") \
    .withColumnRenamed("MANAGER_ID", "Manager_ID") \
    .withColumnRenamed("DEPARTMENT_ID", "Emp_Dept_Emp_ID") 
    
    return employees_modified_df

# This function defines data for employees.
def process_locations_departments_employees_df(spark, locations_df, departments_df, employees_df):
    """
    Processes join between locations_df, departments_df, employees_df DataFrame.
    Args:
        loc_df (DataFrame): The DataFrame containing locations data.
    Returns:
        DataFrame: Processed DataFrame with selected columns.
    """
    loc_dept_emp_df = locations_df.join(departments_df, locations_df["LOCATION_ID"] == departments_df["LOC_ID"], "inner") \
        .join(employees_df, departments_df["DEPT_ID"] == employees_df["Emp_Dept_Emp_ID"], "inner") \
        .select(employees_df["emp_id"], 
                employees_df["first_name"], 
                employees_df["last_name"],
                # locations_df["location_id"],
                departments_df["dept_id"], 
                # departments_df["dept_name"],
                locations_df["city"], 
                locations_df["state_province"],
                employees_df["salary"]
                )
    return loc_dept_emp_df


def avg_salary_per_department(spark, employees_df):
    """
    Calculates the average salary per department from the provided DataFrame.
    Args:
        spark (SparkSession): The current SparkSession.
        loc_dept_emp_df (DataFrame): The DataFrame containing location, department, and employee data.
    Returns:
        DataFrame: DataFrame containing the average salary per department.
    """
    avg_salary_df = employees_df.groupBy("emp_dept_emp_id").agg(round(avg("salary"), 2).alias("avg_salary")).orderBy("avg_salary", ascending=False)
    return avg_salary_df

if __name__ == "__main__":
  print("=== Package: Data_Engineering_1 | Script: department_wise_employees_average_salary ===")

  # Initialize Spark session
  spark = SparkSession.builder \
      .appName("dept_wise_emp_agg") \
      .master("local[3]") \
      .getOrCreate()

  # Suppress unnecessary Spark logging
  spark.sparkContext.setLogLevel("ERROR")

  # Create and display DataFrame
  loc_df = read_from_locations_csv(spark)
  dept_df = read_from_departments_csv(spark)
  emp_df = read_from_employees_csv(spark)

### Create an object to perform join operation between `locations_df`, `departments_df`, `employees_df` process by calling loc_dept_emp_df() ### 
  loc_dept_emp_df = process_locations_departments_employees_df(spark, loc_df, dept_df, emp_df)
  loc_dept_emp_df.show(loc_dept_emp_df.count(),truncate=False)
  print(f"Total Records: {loc_dept_emp_df.count()}")

  avg_salary_per_department_df = avg_salary_per_department(spark, emp_df)
  avg_salary_per_department_df.show(avg_salary_per_department_df.count(), truncate=False)
  print(f"Total Records: {avg_salary_per_department_df.count()}")

  # Stop Spark session
  spark.stop()
