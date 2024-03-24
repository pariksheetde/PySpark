from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("department wise average salary per employee")

spark = SparkSession.builder.appName("department wise average salary per employee").master("local[3]").getOrCreate()

# Define the data for departments
dept_schema = StructType([
    StructField("dept_id", IntegerType(), True),
    StructField("dept_name", StringType(), True)]
  )
columns = ["Dept_ID","Dept_Name"]

dept_data = [(10, "SQL Developer"),
        (15, "PySpark Developer"),
        (20, "Java Developer"),
        (25, "ETL Developer"),
        (30, "AWS Cloud Developer")
        ]

dept_df = spark.createDataFrame(data=dept_data, schema = columns)
# dept_df.show(10, truncate = False)

# Define the data for employees
emp_schema = StructType([
    StructField("emp_id", IntegerType(), True),
    StructField("f_name", StringType(), True),
    StructField("l_name", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("manager_id", IntegerType(), True),
    StructField("dept_id", IntegerType(), True),
    ]
  )
columns = ["Emp_ID", "First_Name", "Last_Name", "Salary", "Manager_ID", "Dept_ID"]

emp_data = [(100, "Steve", "Smith", 2500012, None, 10),
            (110, "Brian", "Lara", 2500147, 100, 10),
            (120, "Steve", "Jobs", 1245000, 110, 10),
            (130, "Peter", "Croft", 1000000, 120, 15),
            (140, "Daren", "Walker", 1450000, 130, 15),
            (150, "Brian", "Adams", 1750000, 140, 15),
            (160, "Dwyane", "Jhonson", 2600000, 150, 20),
            (170, "Monica", "Bellucci", 3125000, 160, 20),
            (180, "Kate", "Moss", 3985000, 170, 20),
        ]

emp_df = spark.createDataFrame(data=emp_data, schema = columns)
# emp_df.show(10, False)

# Need to select those employees whose salary is greater than the average salary in their respective department
# Calculate average salary for each department from emp_df
agg_avg_sal_dpt_df = emp_df.groupby("dept_id") \
    .agg(
            avg("salary").alias("avg_salary")
)
# agg_avg_sal_dpt_df.show(10, False)

col_rename = emp_df.withColumnRenamed("Dept_ID", "emp_dept_id")

avg_sal_dept_df = col_rename.join(agg_avg_sal_dpt_df, col_rename.emp_dept_id == agg_avg_sal_dpt_df.dept_id, "inner") \
    .join(dept_df, dept_df.Dept_ID == col_rename.emp_dept_id, "inner") \
    .withColumnRenamed("Dept_ID", "emp_dept_id") \
    .select(col_rename["emp_id"], col_rename["first_name"],
            col_rename["last_name"], col_rename["salary"], col_rename["emp_dept_id"].alias("dept_id"),
            dept_df["Dept_Name"].alias("dept_name"),
            agg_avg_sal_dpt_df["avg_salary"]) \
    .where(col_rename["salary"] > agg_avg_sal_dpt_df["avg_salary"]) \
    .withColumn("difference", col_rename["salary"] - agg_avg_sal_dpt_df["avg_salary"]) \
    .show(10, truncate = False)


spark.stop()

