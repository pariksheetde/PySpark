from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 2")

spark = SparkSession.builder.appName("Movies Data Analysis 2").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json"))

movies_df.show(10, False)

# select only the required columns
movies_df_1 = movies_df.select(col("Title"), col("Director"), col("IMDB_Rating"),
    col("Major_Genre"), col("Production_Budget"), col("Release_Date"), col("Rotten_Tomatoes_Rating"),
    col("US_DVD_Sales"), col("US_Gross")) \
    .fillna(value = 0,subset =["Production_Budget", "US_DVD_Sales"]) \
    .fillna(value = "No Director",subset =["Director"])

print(f"calculate gross_collection, profit for each movie")
profit_df = movies_df_1.select(col("Title"), col("Director"), col("Production_Budget"),
      col("US_DVD_Sales"), col("US_Gross"),
      (col("US_DVD_Sales") + col("US_Gross")).alias("Gross_Collection")) \
      .withColumn("Profit", col("Gross_Collection") - col("Production_Budget")) \


profit_df.show(truncate=False)
spark.stop()