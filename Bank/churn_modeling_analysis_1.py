from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == "__main__":
    print("Package : Bank, Script : Churn Modeling 1")

spark = SparkSession.builder.appName("Churn Modeling 1").master("local[3]").getOrCreate()

churn_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(r"D:\DataSet\DataSet\SparkDataSet\ChurnModeling.csv")

print(f"Records Effected {churn_df.count()}")

# select the required columns

churn_agg = churn_df.select(col("CustomerID"), col("Surname"), col("CreditScore"), col("Geography"), col("Gender"), \
                            col("Age")) \
                    .filter("Geography == 'France'") \
                    .filter("CreditScore >= 501") \
                    .where("Gender == 'Male'")
churn_agg.show()

print(f"Records Effected {churn_agg.count()}")
spark.stop()