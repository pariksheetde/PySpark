from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_4, Script: Sales Data Analysis 4")

spark = SparkSession.builder.appName("Sales Data Analysis 4").master("local[3]").getOrCreate()

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

# compute the highest sales for category and sub-category
print("Top Sales in each city, category, sub_category")
sales_cat_subcat = sales_df.select("City", "Category", "Sub_Category", "Sales") \
      .withColumn("Rank",row_number().over(Window.partitionBy("City", "Category", "Sub_Category").orderBy(desc(col("Sales"))))) \
      .sort(desc(col("Sales"))) \
      .where("Category = 'Technology' and Sub_Category = 'Copiers' and City = 'Los Angeles'")

sales_cat_subcat.show(10,False)

# select those customers who have made maximum purchase for each OrderID, Category
print("select those customers who have made maximum purchase for each OrderID, Category")
purchase_order_category_cnt = sales_df.select("CustomerID",  "OrderID", "Category",  "Sales") \
      .withColumn("Max_Sales", max("Sales").over(Window.partitionBy("OrderID", "Category").orderBy(desc(col("Sales"))))) \
      .withColumn("Rank", rank().over(Window.partitionBy("OrderID", "Category").orderBy(desc(col("Sales"))))) \
      .where("CustomerID IN ('AA-10315')")
purchase_order_category_cnt.show(10, False)

# select those customers who have made maximum purchase for each OrderID
print("select those customers who have made maximum purchase for each OrderID")
puchase_order_cnt = sales_df.select("CustomerID",  "OrderID", "Category",  "Sales") \
      .withColumn("Max_Sales", max("Sales").over(Window.partitionBy("OrderID").orderBy(desc(col("Sales"))))) \
      .withColumn("Rank", rank().over(Window.partitionBy("OrderID").orderBy(desc(col("Sales"))))) \
      .where("CustomerID IN ('AA-10315')")
puchase_order_cnt.show(10, False)

# select those customers who have made maximum purchase for each OrderID
print("select those customers who have made maximum purchase for each OrderID")
sum_sales = sales_df.select("CustomerID",  "OrderID", "Category",  "Sales") \
      .withColumn("Sum_Sales", sum("Sales").over(Window.partitionBy("CustomerID").orderBy(desc(col("Sales"))))) \
      .withColumn("Rank", rank().over(Window.partitionBy("CustomerID").orderBy(desc(col("Sum_Sales"))))) \
      .where("CustomerID IN ('AA-10315', 'AA-10375')")
sum_sales.show(10, False)

spark.stop()