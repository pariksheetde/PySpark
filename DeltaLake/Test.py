from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Test.py")

spark = SparkSession.builder.appName("Test.py").master("local[3]").getOrCreate()

invoice_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    # .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkData/Invoices.csv"))

invoice_df.printSchema()
invoice_df.show(truncate = False)

# invoice_df.write.format("delta").mode('overwrite').save("D:/DataSet/OutputDataset/Delta/flight_data")

spark.stop()