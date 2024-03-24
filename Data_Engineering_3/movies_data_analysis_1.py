from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 1")

spark = SparkSession.builder.appName("Movies Data Analysis 1").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json"))

movies_df.show(truncate=False)

movies_df_1 = movies_df.select(col("Title"), col("Director"), col("IMDB_Rating"),
      col("Major_Genre"), col("Production_Budget"), col("Release_Date"), col("Rotten_Tomatoes_Rating"),
      col("US_DVD_Sales"), col("US_Gross")) \
    .fillna(value = 0,subset =["Production_Budget", "US_DVD_Sales", "US_Gross"])

movies_df_1.show(truncate=False)


print(f"Count of Movies / Title in each Genre")
genre_cnt = movies_df_1.groupBy("Major_Genre") \
      .agg(
        count("Major_Genre").alias("Cnt_Genre")
      ) \
      .sort(col("Cnt_Genre").desc())

genre_cnt.show(truncate = False)

print(f"Total earning for each movie")

profit = movies_df_1.select(col("Title"), col("Major_Genre"), col("IMDB_Rating"),
      col("Production_Budget"), col("Rotten_Tomatoes_Rating"),
      col("US_DVD_Sales"), col("US_Gross"),
      (col("US_DVD_Sales") + col("US_Gross")).alias("Total"))

profit.show(truncate=False)

spark.stop()