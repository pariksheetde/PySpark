from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_3, Script : Movies Data Analysis 9")

spark = SparkSession.builder.appName("Movies Data Analysis 9").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json")
    .fillna(value="Not Listed", subset= ["Director"])
    .fillna(value= "Not Listed", subset = ["Major_Genre"])
    .fillna(value= 0, subset = ["US_DVD_Sales", "US_Gross", "IMDB_Rating", "Rotten_Tomatoes_Rating"]))

sel_movies_df = movies_df.select(col("Major_Genre"), col("Title"), col("Director"),
      (coalesce(col("IMDB_Rating"), col("Rotten_Tomatoes_Rating") / 10)).alias("Rating")) \
      .where("Rating != 0")

sel_movies_df.show(10, False)

winSpec = Window.partitionBy("Major_Genre").orderBy(col("Rating").asc())

rank_df = sel_movies_df.withColumn("rank", rank().over(winSpec)).where("rank = 1")
rank_df.show(10, False)

spark.stop()