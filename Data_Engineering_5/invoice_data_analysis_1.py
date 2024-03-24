from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Invoice Data Analysis 1")

spark = SparkSession.builder.appName("Invoice Data Analysis 1").master("local[3]").getOrCreate()

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

print("COMPUTE THE INVOICE STATS")
inv_agg = invoice_df.select("Country", "InvoiceNo") \
      .groupBy("Country") \
      .agg(
        count("Country").alias("Cnt_of_Records"),
        countDistinct("Country").alias("Number_of_Country"),
        count("InvoiceNo").alias("Number_of_Invoices_each_Country")
)
inv_agg.show(truncate = False)

cnt_invoice_country = invoice_df.groupBy("Country", "InvoiceNo") \
      .agg(
        count("InvoiceNo").alias("Invoice_Cnt")
    )
cnt_invoice_country.show(truncate = False)

spark.stop()