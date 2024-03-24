from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 5")

spark = SparkSession.builder.appName("Movies Data Analysis 5").master("local[3]").getOrCreate()

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
    .fillna(value= 0, subset = ["IMDB_Rating", "Rotten_Tomatoes_Rating"])

movies_df.show(10, truncate=False)

print(f"Count Number of movies for each director in each Genre")

cnt_directors = movies_df.select(col("Director"), col("Major_Genre")) \
      .groupBy(col("Director"), col("Major_Genre")) \
      .agg(
        count("Major_Genre").alias("Cnt_of_Movies_Genre")
      ) \
    .sort(col("Director").asc_nulls_last())

cnt_directors.show(10, False)
spark.stop()