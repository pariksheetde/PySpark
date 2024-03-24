from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 4")

spark = SparkSession.builder.appName("Sales Data Analysis 4").master("local[3]").getOrCreate()

sales_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkDataSet/Sales_Data.csv"))

print("COMPUTE THE PRODUCT FOR YEAR STATUS IN EACH YEAR")
status_df = sales_df.selectExpr("PRODUCTLINE as Product", "STATUS as Status", "Sales as Sales") \
      .groupBy("Product", "Status") \
      .agg(
        sum("Sales").alias("Sum_Sales")
      ).orderBy(desc_nulls_last("Sum_Sales"))
status_df.show(10, False)

temp_status = status_df.createOrReplaceTempView("Status_Temp")
top_status = spark.sql("""
                    SELECT
                    product, status, sum_sales,
                    rank() over (partition by product order by sum_sales desc) as Rank
                    from status_temp
                    order by product asc, sum_sales desc""")
top_status.show(10,False)

spark.stop()