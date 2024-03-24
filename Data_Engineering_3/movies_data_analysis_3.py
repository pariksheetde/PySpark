from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 3")

spark = SparkSession.builder.appName("Movies Data Analysis 3").master("local[3]").getOrCreate()

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

print(f"Calculate Ratings for each Movie")

ratings_df_cln_1 = movies_df.select("Title", "Director", "IMDB_Rating", "Rotten_Tomatoes_Rating", "Major_Genre") \
    .withColumn("RT_Rating", col("Rotten_Tomatoes_Rating") / 10) \
    .fillna(value = "N/A", subset =["Major_Genre"]) \
    .fillna(value= 0, subset= ["RT_Rating", "IMDB_Rating"]) \
    .drop("Rotten_Tomatoes_Rating")

ratings_df_cln_2 = ratings_df_cln_1.select(col("Title"),
    col("Director"), col("IMDB_Rating"), col("RT_Rating"),
    col("Major_Genre"),
    coalesce(col("IMDB_Rating"), col("RT_Rating")).alias("Rating")) \
    .sort(col("Rating").desc_nulls_last())

ratings_df_cln_2.show(10, False)

spark.stop()