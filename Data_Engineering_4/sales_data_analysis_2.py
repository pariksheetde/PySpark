from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: Sales Data Analysis 2")

spark = SparkSession.builder.appName("Sales Data Analysis 2").master("local[3]").getOrCreate()

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

# calculate total sales for each category
sales_category = sales_df.select("Category", col("Sales")) \
      .groupBy("Category") \
      .agg(
        sum(col("Sales")).alias("Sum_Sales_Category") \
      ).sort(desc_nulls_last(col("Category")))
sales_category.show(10, False)

# calculate total sales for each category and sub-category
print("Sum of sales in each category and sub_category")
sales_cat_sub_cat = sales_df.select("Category", "Sub_Category", "Sales") \
      .groupBy(col("Category"), col("Sub_Category")) \
      .agg(
        round(sum("Sales"), 2).alias("Sum_Sales_Sub_Category") \
      ).sort(desc_nulls_last(col("Category")))
sales_cat_sub_cat.show(10, False)

# number of customers in each city
cust_cnt = sales_df.select("CustomerID", col("City"), col("Sales")) \
      .groupBy(col("City")) \
      .agg(
        count("CustomerID").alias("Cust_Cnt"),
        round(sum(col("Sales")), 2).alias("Sum_Sales_City")
      ).orderBy(desc_nulls_last(col("Cust_Cnt")))
cust_cnt.show(10, False)

# find the sum of sales for each category where City is Tyler
sum_sales_tyler = sales_df.select(col("City"), "Category", "Sales") \
      .groupBy("City") \
      .agg(
        sum("Sales").alias("Sum_Sales")
      ) \
      .where("City = 'Tyler'")
sum_sales_tyler.show(10, False)

spark.stop()