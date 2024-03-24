from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 4")

spark = SparkSession.builder.appName("Movies Data Analysis 4").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json")) \
    .fillna(value="N/A", subset= ["Director"]) \
    .fillna(value= 0, subset = ["US_DVD_Sales", "US_Gross"])

movies_df.show(10, False)

print(f"Count Number of directors")
cnt_directors = movies_df.select(col("Director")) \
      .agg(
        countDistinct("Director").alias("Cnt_Directors")
      )
cnt_directors.show(10, False)

print(f"calculate the sum of US_DVD_Sales + US_Gross for each movie")
profit = movies_df.select(col("Title"), col("Director"),
                               col("US_DVD_Sales"), col("US_Gross"),
                               (col("US_DVD_Sales") + col("US_Gross")).alias("Profit")
                               )
profit.show(truncate = False)

print(f"calculate the sum of collections for each director and number of movies")
cnt_movies_per_dir = profit.select(col("Director"), col("Title"), col("Profit")) \
    .groupBy("Director") \
    .agg(
        count(col("Title")).alias("Number_of_Movies"),
        sum(col("Profit")).alias("Collection")
) \
.sort(col("Number_of_Movies").desc_nulls_last())
cnt_movies_per_dir.show(truncate=False)

spark.stop()