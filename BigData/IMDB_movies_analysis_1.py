from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType
from pyspark.sql.functions import col

if __name__ == "__main__":
    print("Package : BigData, Script : IMDB Movies Analysis 1")

spark = SparkSession.builder.appName("IMDb Movies Analysis 1").master("local[3]").getOrCreate()

movies_schema = StructType([
    StructField("imdb_title_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("original_title", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("date_published", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("country", StringType(), True),
    StructField("language", StringType(), True),
    StructField("director", StringType(), True),
    StructField("writer", StringType(), True),
    StructField("production_company", StringType(), True),
    StructField("actors", StringType(), True),
    StructField("description", StringType(), True),
    StructField("avg_vote", StringType(), True),
    StructField("votes", StringType(), True),
    StructField("budget", StringType(), True),
    StructField("usa_gross_income", StringType(), True),
    StructField("worlwide_gross_income", StringType(), True),
    StructField("metascore", StringType(), True),
    StructField("reviews_from_users", StringType(), True),
    StructField("reviews_from_critics", StringType(), True)
]
)

spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
movies_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .schema(movies_schema) \
    .option("mode", "permissive") \
    .option("sep", ",") \
    .option("nullValue", "") \
    .option("compression", "snappy") \
    .option("path","D:/DataSet/DataSet/SparkDataSet/IMDB_Movies.csv") \
    .load()

movies_sel = movies_df.select(col("title"), expr("year"), col("date_published"),
      col("genre"), col("duration"), col("country"), col("language"),
      col("director"), expr("reviews_from_users as users_rating"),
      expr("reviews_from_critics as critics_rating")) \
      .filter("country = 'USA'") \
      .where("year > 2000")

movies_sel.show(10, False)
print(f"Number of Records Fetched {movies_sel.count()}")

spark.stop()