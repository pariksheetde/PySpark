from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Sales Data Analysis 3")

spark = SparkSession.builder.appName("Sales Data Analysis 3").master("local[3]").getOrCreate()

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

print("COMPUTE THE STATUS FOR YEAR PRODUCT IN EACH YEAR")
status = sales_df.selectExpr( "YEAR_ID as Year", "STATUS as Status", "PRODUCTLINE as Product", "sales as Sales") \
      .groupBy("Year","Status", "Product") \
      .agg(
        sum("sales").alias("Total_Sales")
      ).orderBy(desc("Total_Sales"))
status.show(10, False)

tab_status = status.createOrReplaceTempView("Status_Temp")
sql_status = spark.sql("""select Year, Status, Product,
                    Total_Sales,
                    rank() over (partition by year, status order by total_sales desc) as Rank
                    from Status_Temp
                    order by year asc, total_sales desc""")
sql_status.show(100,False)

spark.stop()