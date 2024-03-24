from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_2, Script : Department Location Analysis 1")

spark = SparkSession.builder.appName("Dept_Loc_Analysis_1").master("local[3]").getOrCreate()

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


loc_dept_join = loc_df.join(dept_df, loc_df.LOCATION_ID == dept_df.LOC_ID) \
            .select(loc_df["location_id"], loc_df["country_id"], dept_df["dept_id"],
                    loc_df["city"], dept_df["dept_name"])

loc_dept_join.show(10, False)
print(f"Number of Records {loc_dept_join.count()}")

spark.stop()