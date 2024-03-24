from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: Sales Data Analysis 3")

spark = SparkSession.builder.appName("Sales Data Analysis 3").master("local[3]").getOrCreate()

# define schema for sales
sales_schema = StructType(fields = [
        StructField("RowID", IntegerType(), True),
        StructField("OrderID", StringType(), True),
        StructField("Order_Date", StringType(), True),
        StructField("Ship_Date", StringType(), True),
        StructField("Ship_Mode", StringType(), True),
        StructField("CustomerID", StringType(), True),
        StructField("Customer_Name", StringType(), True),
        StructField("Segment", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("City", StringType(), True),
        StructField("State", StringType(), True),
        StructField("Postal_Code", IntegerType(), True),
        StructField("Region", StringType(), True),
        StructField("Product_ID", StringType(), True),
        StructField("Category", StringType(), True),
        StructField("Sub_Category", StringType(), True),
        StructField("Product_Name", StringType(), True),
        StructField("Sales", DoubleType(), True)
    ])

sales_df = (spark.read
    .format("csv")
    .option("header", "true")
    .schema(sales_schema)
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Tesco_Sales.csv"))

sales_df.printSchema()
sales_df.show(10, False)
print(f"Number of records in the sales_df DF: {sales_df.count()}")

# # Detroit, New York City, Chicago, Dallas, Jacksonville
top_city_sales_1 = sales_df.select("City", "Category", "Sales") \
      .withColumn("Max_Sales", max("Sales").over(Window.partitionBy("City").orderBy(desc(col("Sales"))))) \
      .withColumn("Rank", row_number().over(Window.partitionBy("City").orderBy(desc(col("Max_Sales"))))) \
      .where("City = 'Jacksonville'") \
      .orderBy(desc(col("Sales")))
top_city_sales_1.show(10, False)

spark.stop()