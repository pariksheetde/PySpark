from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


if __name__ == "__main__":
    print("Package : BigData, Script : Cars Data Analysis 1")

spark = SparkSession.builder.appName("USA Cars Details using DataFrame 1").master("local[3]").getOrCreate()

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

cars = cars_df.selectExpr("ID", "price as Price", "brand as Brand", "year as YYYY", "title_status as Title_Status",
                          "mileage as Mileage", "color as Color", "lot as Lot", "state as State",
                          "country as Country", "condition as Condition")

cars.write.format("json") \
      .mode("Overwrite") \
      .option("header", "true") \
      .option("path", "D:/DataSet/OutputDataset/Cars/") \
      .save()

cars_final_df = spark.read \
      .format("json") \
      .option("header", "true") \
      .option("mode", "failFast") \
      .option("dateFormat", "YYYY-MM-dd") \
      .option("path","D:/DataSet/OutputDataset/Cars/") \
      .load()

cars_final_df.show()
spark.stop()