from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: Sales Data Analysis 1")

spark = SparkSession.builder.appName("Sales Data Analysis 1").master("local[3]").getOrCreate()

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

# count number of countries
print("Number of Countries")
cnt_distinct_country = sales_df.select("Country") \
    .groupBy("Country") \
    .agg(
        count(col("Country")).alias("Country_Cnt")
)
cnt_distinct_country.show(10, False)

# count number of cities
print("Number of Cities")
cnt_distinct_city = sales_df.select("City") \
    .groupBy("City") \
    .agg(
        count(col("City")).alias("City_Cnt")
    ).sort(desc_nulls_last(col("City_Cnt")))
cnt_distinct_city.show(10, False)

# count number of Segment
print("Number of Segment")
cnt_segment = sales_df.select("Segment") \
    .groupBy("Segment") \
    .agg(
        count(col("Segment")).alias("Segment_Cnt")
    ).sort(desc_nulls_last(col("Segment_Cnt")))
cnt_segment.show(10, False)

# count number of Ship Mode
print("Number of Ship Mode")
cnt_ship_mode = sales_df.select("Ship_Mode") \
    .groupBy("Ship_Mode") \
    .agg(
        count(col("Ship_Mode")).alias("ShipMode_Cnt")
    ).sort(desc_nulls_last(col("ShipMode_Cnt")))
cnt_ship_mode.show(10, False)

# count number of Customers
print("Number of Customers")
cnt_customer = sales_df.select("CustomerID") \
    .groupBy("CustomerID") \
    .agg(
        count(col("CustomerID")).alias("Customer_Cnt")
    ).sort(desc_nulls_last(col("Customer_Cnt")))
cnt_customer.show(10, False)

# count number of orders received
print("Number of Orders Received")
cnt_orders = sales_df.select("OrderID") \
    .groupBy("OrderID") \
    .agg(
        count(col("OrderID")).alias("Orders_Cnt")
    ).sort(desc_nulls_last(col("Orders_Cnt")))
cnt_orders.show(10, False)

# count number of orders category
print("Number of Product Category")
cnt_category = sales_df.select("Category") \
    .groupBy("Category") \
    .agg(
        count(col("Category")).alias("Category_Cnt")
    ).sort(desc_nulls_last(col("Category_Cnt")))
cnt_category.show(10, False)

# count number of orders sub category
print("Number of Product Category")
cnt_sub_category = sales_df.select("Sub_Category") \
    .groupBy("Sub_Category") \
    .agg(
        count(col("Sub_Category")).alias("Sub-Category_Cnt")
    ).sort(desc_nulls_last(col("Sub-Category_Cnt")))
cnt_sub_category.show(10, False)

# calculate sales in each city
city_sales = sales_df.select("City", "Category", "Sales") \
      .groupBy("City") \
      .agg(
        sum("Sales").alias("Sum_Sales")
      ) \
      .where("City = 'Tyler'") \
      .sort(desc(col("Sum_Sales")))
city_sales.show(10, False)

spark.stop()