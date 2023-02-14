from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Movies Data Analysis 16")

spark = SparkSession.builder.appName("Movies Data Analysis 16").master("local[3]").getOrCreate()

movies_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json"))

null_df = movies_df.fillna(value= 0, subset = ["US_Gross", "US_DVD_Sales"])

print(f"best movies based on collection")

movies_coll_df = null_df.select(col("Title"), col("US_DVD_Sales"), col("US_Gross"),
      (col("US_DVD_Sales") + col("US_Gross")).alias("Collection"))

# movies_coll_df.show(10, False)

print(f"Best Movies - Collection")
best_movies_df = movies_coll_df.select("Title", "Collection") \
      .withColumn("Rank", rank().over(Window.orderBy("Collection").orderBy(col("Collection").desc())))

best_movies_df.show(10, False)

print(f"Worst Movies - Collection")
worst_movies_df = movies_coll_df.select("Title", "Collection") \
      .filter("Collection != 0") \
      .withColumn("Rank", rank().over(Window.orderBy("Collection").orderBy(col("Collection").asc())))

worst_movies_df.show(10, False)

print(f"Best &  Worst Movie - Collection")
best_worst_movie = best_movies_df.union(worst_movies_df) \
      .where("Rank = 1")
best_worst_movie.show(10, False)

spark.stop()
