from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 2")

spark = SparkSession.builder.appName("Sales Data Analysis 2").master("local[3]").getOrCreate()

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

print("COMPUTE THE STATS FOR SALES DATA FOR EACH PRODUCT")
print("Compute Sales for all products in the year 2003")

qty_cnt = sales_df.createOrReplaceTempView("Sales_I")
sales_product_i = spark.sql("""SELECT * FROM
            (SELECT
            year_id as year,
            productline as product,
            sales,
            max(sales) over(partition by year_id, productline order by sales desc) as sales,
            rank() over(partition by year_id, productline order by sales desc) as rank
            from Sales_I
            order by year_id, sales desc) Temp WHERE rank = 1""")
sales_product_i.show(truncate = False)

print("Compute Sales for all products in each year")
purchase_qty = sales_df.createOrReplaceTempView("Sales_II")

sales_product_ii = spark.sql("""
                        SELECT
                        year, product,
                        max(sum_sales) over (partition by year, product) as max_sales,
                        rank() over (partition by year, product order by sum_sales) as outer_rank
                        FROM
                        (SELECT
                        year_id as year,
                        productline as product,
                        sum(sales) over(partition by year_id, productline order by sales asc) as sum_sales,
                        rank() over(partition by year_id, productline order by sales asc) as rank
                        from sales_ii
                        order by year_id, sum_sales desc)
                        temp""")
sales_product_ii.show(truncate=False)

print("Number of status for each product purchased in each year")

top_sales = sales_product_ii.filter("outer_rank = 1") \
    .orderBy(desc("Year"))
top_sales.show(100, False)

spark.stop()