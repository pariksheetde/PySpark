from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 1")

spark = SparkSession.builder.appName("Sales Data Analysis 1").master("local[3]").getOrCreate()

sales_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Sales_Data.csv"))

sales_df.show(truncate=False)

print("COMPUTE THE STATS FOR SALES DATA")
print("Number of Orders made in each year")

qty_cnt = sales_df.createOrReplaceTempView("Sales_Details")
qty_cnt_df = spark.sql("""
            SELECT
            year_id,
            count(quantityordered) as qty_cnt
            from sales_details
            group by year_id
            """)
qty_cnt_df.show(truncate = False)

print("Number of products purchased in each year")
purchase_qty = sales_df.createOrReplaceTempView("Purchase_Details")

purchase_qty_df = spark.sql("""
                SELECT
                year_id,
                count(productline) as product_cnt,
                productline
                from purchase_details
                group by year_id, productline
                order by year_id""")
purchase_qty_df.show(truncate=False)

print("Number of status for each product purchased in each year")
status_purchase = sales_df.createOrReplaceTempView("Status_Details")

status_purchase_df = spark.sql("""
            SELECT
            year_id,
            productline as product_name,
            status,
            count(productline) as product_cnt
            from status_details
            group by year_id, status, product_name
            order by year_id""")

status_purchase_df.show(truncate = False)

spark.stop()