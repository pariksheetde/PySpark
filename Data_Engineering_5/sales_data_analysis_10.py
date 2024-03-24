from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 10")

spark = SparkSession.builder.appName("Sales Data Analysis 10").master("local[3]").getOrCreate()

sales_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Sales_Data.csv"))

sales_df.printSchema()
sales_df.show(10, False)

print("COMPUTE MAX SALES IN EACH COUNTRY FOR EACH PRODUCT IN EACH YEAR")
order_df = sales_df.createOrReplaceTempView("Sales_Temp")
order_stat_df = spark.sql("""
                    SELECT
                    year_id as Year,
                    productline as Product,
                    ordernumber as Order,
                    sum(sales) over (partition by ordernumber, productline order by sales desc) as Total_Sales,
                    rank() over (partition by ordernumber, productline order by sales desc) as Rank
        from sales_temp""")
order_stat_df.show(100,False)

spark.stop()
