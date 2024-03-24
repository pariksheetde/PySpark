from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, ArrayType
from pyspark.sql.functions import col

if __name__ == "__main__":
    print("Package : BigData, Script : Stock_Price_Analysis_1")

spark = SparkSession.builder.appName("Stock_Price_Analysis_1").master("local[3]").getOrCreate()

stock_schema = StructType([
    StructField("StkDate", StringType()),
    StructField("Open", DoubleType()),
    StructField("High", DoubleType()),
    StructField("Low", DoubleType()),
    StructField("Close", DoubleType()),
    StructField("AdjClose", DoubleType()),
    StructField("Volume", DoubleType())
  ])

stock_df = (spark.read
            .format("csv")
            .option("header", "true")
            .schema(stock_schema)
            .option("dateFormat", "YYYY-dd-MM")
            .option("sep", ",")
            .option("nullValue", "")
            .option("compression", "snappy")
            .option("path","D:/DataSet/DataSet/SparkDataSet/StockPrice.csv")
            .load())

stock_df.printSchema()
clean_stk_df = stock_df.select(date_format("StkDate", "MM/dd/yyy").alias("StkDate"), "StkDate",
                               "Open", "High", "Low", "Close", "AdjClose", "Volume")
clean_stk_df.show(10, False)

spark.stop()