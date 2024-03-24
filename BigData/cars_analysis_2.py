from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : BigData, Script : Cars Data Analysis 2")

spark = SparkSession.builder.appName("USA Cars Details using DataFrame 2").master("local[3]").getOrCreate()

# define schema for cars
cars_schema = StructType([
StructField("ID", IntegerType()),
    StructField("price", IntegerType()),
    StructField("brand", StringType()),
    StructField("model", StringType()),
    StructField("year", IntegerType()),
    StructField("title_status", StringType()),
    StructField("mileage", StringType()),
    StructField("color", StringType()),
    StructField("vin", StringType()),
    StructField("lot", IntegerType()),
    StructField("state", StringType()),
    StructField("country", StringType()),
    StructField("condition", StringType())

])
cars_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(cars_schema) \
    .option("mode", "failFast") \
    .option("compression", "snappy") \
    .option("dateFormat", "YYYY-MM-dd") \
    .option("path","D:/DataSet/DataSet/SparkDataSet/cars_USA.csv") \
    .load()

analyzed_cars_df = cars_df.selectExpr("ID", "initcap(brand) as Brand", "initcap(model) as Model", "year",
                                      "title_status as Title_Status", "mileage as Mileage", "initcap(color) as Color",
                                      "vin", "lot", "initcap(state) as State", "upper(country) as Country", "condition as Condition")

analyzed_cars_df.printSchema()
analyzed_cars_df.show(10, False)

spark.stop()