from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import expr, to_date, when, col, desc, row_number


def create_employees_dataframe(spark):
    """
    Creates a PySpark DataFrame containing customer details.
    Args:
        spark (SparkSession): The current SparkSession.
    Returns:
        DataFrame: Customer DataFrame with pre-defined schema.
    """

    emp_columns = ["Emp_ID", "First_Name", "Last_Name", "Salary", "Manager_ID", "Dept_ID"]

    emp_data = [(100, "Steve", "Smith", 2500012, None, 10),
            (110, "Brian", "Lara", 2500147, 100, 10),
            (120, "Steve", "Jobs", 1245000, 110, 10),
            (130, "Peter", "Croft", 1000000, 120, 15),
            (140, "Daren", "Walker", 1450000, 130, 15),
            (150, "Brian", "Adams", 1750000, 140, 15),
            (160, "Dwyane", "Jhonson", 2600000, 150, 20),
            (170, "Monica", "Bellucci", 3125000, 160, 20),
            (180, "Kate", "Moss", 3985000, 170, 20),
            (190, "Emma", "Watson", 4500000, 180, 30),
            (200, "Scarlett", "Johansson", 5000000, 190, 30),
            (210, "Natalie", "Portman", 6000000, 200, 30),
            (220, "Jennifer", "Lawrence", 7000000, 210, 30),
        ]

    emp_df = spark.createDataFrame(data=emp_data, schema=emp_columns)
    return emp_df

# This function defines data for departments.
def create_departments_dataframe(spark):
    """
    Defines data for departments.
    Args:
        spark (SparkSession): The current SparkSession.
    Returns:
        DataFrame: Departments DataFrame with pre-defined schema
    """
    dept_schema = StructType([
      StructField("dept_id", IntegerType(), True),
      StructField("dept_name", StringType(), True)]
    )
    dept_columns = ["Dept_ID","Dept_Name"]

    dept_data = [(10, "SQL Developer"),
          (15, "PySpark Developer"),
          (20, "Java Developer"),
          (25, "ETL Developer"),
          (30, "AWS Cloud Developer")
          ]

    dept_df = spark.createDataFrame(data=dept_data, schema=dept_columns)

    return dept_df

def create_emp_dept_agg_df(emp_df, dept_df):
    """
    Creates a DataFrame that aggregates employee data by department.
    Args:
        emp_df (DataFrame): DataFrame containing employee details.
        dept_df (DataFrame): DataFrame containing department details.
    Returns:
        DataFrame: Aggregated DataFrame with employee count and average salary per department.
    """
    emp_dept_agg_df = emp_df.join(dept_df, emp_df.Dept_ID == dept_df.Dept_ID, "inner") \
        .groupBy(dept_df["Dept_ID"]) \
        .agg(
            round(avg("Salary"),2).alias("Avg_Salary")
        ) \
        .orderBy(desc("Avg_Salary"))

    return emp_dept_agg_df

def create_dept_avg_salary_grt_df(emp_dept_agg_df, emp_df):
    """
    Creates a DataFrame that shows emplyess whose average salary is greater than average salary by department.
    Args:
        emp_dept_agg_df (DataFrame): DataFrame containing aggregated employee data by department.
    Returns:
        DataFrame: DataFrame with employee details whose salary is greater than average salary within their respective department ID.
    """
    dept_avg_salary_df = emp_dept_agg_df.join(emp_df, emp_dept_agg_df.Dept_ID == emp_df.Dept_ID, "inner") \
        .select('Emp_ID', 'First_Name', 'Last_Name', dept_df['Dept_ID'], 'Salary', 'Avg_Salary') \
        .where(col('Salary') > col('Avg_Salary')) \
        .orderBy(desc("Avg_Salary"))
    
    final_df = dept_avg_salary_df.join(dept_df, dept_avg_salary_df.Dept_ID == dept_df.Dept_ID, "inner") \
        .select('Emp_ID','First_Name', 'Last_Name', 'Dept_Name', 'Salary', 'Avg_Salary') \
        .withColumn('Difference', round(col('Salary') - col('Avg_Salary'),2)) \
        .orderBy(desc("Difference"), asc("Emp_ID")) \

    return final_df

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
  emp_df = create_employees_dataframe(spark)
  dept_df = create_departments_dataframe(spark)
  print("DataFrame:")

  print("=== Employees DataFrame ===")
  emp_df.printSchema()
  emp_df.select("*").show(n = 10, truncate=False)

  print("=== Departments DataFrame ===")
  dept_df.printSchema()
  dept_df.select("*").show(n = 10, truncate=False)

  print("=== Aggregated Employee Data by Department ===")
  emp_dept_agg_df = create_emp_dept_agg_df(emp_df, dept_df)
  emp_dept_agg_df.printSchema()
  emp_dept_agg_df.select("*").show(emp_dept_agg_df.count(), truncate=False)

  print("=== Departments with Average Salary Greater than Department Average ===")
  dept_avg_salary_df = create_dept_avg_salary_grt_df(emp_dept_agg_df, emp_df)
  dept_avg_salary_df.printSchema()
  dept_avg_salary_df.select("*").show(dept_avg_salary_df.count(), truncate=False)

  # Stop Spark session
  spark.stop()