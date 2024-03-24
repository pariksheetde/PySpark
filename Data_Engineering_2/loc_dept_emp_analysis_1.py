from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_2, Script : Locations wise Employees Analysis on Departments")

spark = SparkSession.builder.appName("Locations wise Employees Analysis on Departments").master("local[3]").getOrCreate()

loc_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .option("mode", "PERMISSIVE") \
    .option("inferSchema", "true") \
    .option("nullValue", "NA") \
    .option("sep", ",") \
    .option("compression", "snappy") \
    .option("dateFormat", "dd/MM/yyy") \
    .load("D:/DataSet/DataSet/SparkDataSet/locations.csv")

# loc_df.printSchema()

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

# dept_df.printSchema()

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

# emp_df.printSchema()

loc_tab = loc_df.createOrReplaceTempView("Locations")
dept_tab = dept_df.createOrReplaceTempView("Departments")
emp_tab = emp_df.createOrReplaceTempView("Employees")

SQL_qry = spark.sql(
      """
        select e.employee_id, e.first_name||' '||e.last_name as Name,
        l.location_id, d.dept_id, d.Dept_Name, l.city as City, l.state_province as State
        from
        Locations l join departments d
        on l.location_id = d.loc_id join employees e
        on e.department_id = d.dept_id
        """)

SQL_qry.show(truncate = False)
print(f"Records Effected: {SQL_qry.count()}")

spark.stop()