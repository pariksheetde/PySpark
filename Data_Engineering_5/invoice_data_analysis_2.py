from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Package : Data_Engineering_5, Script : Invoice Data Analysis 2")

spark = SparkSession.builder.appName("Invoice Data Analysis 2").master("local[3]").getOrCreate()

invoice_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE")
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy")
    .csv("D:/DataSet/DataSet/SparkData/Invoices.csv"))

invoice_df.printSchema()
invoice_df.show(truncate = False)

print("COMPUTE THE INVOICE STATS")
cln_invoice_df = (invoice_df.selectExpr("InvoiceNo", "Quantity", "InvoiceDate", "UnitPrice", "Country")
      .withColumn("Invoice_DD", substring(col("InvoiceDate"), 1, 2))
      .withColumn("Invoice_MM", substring(col("InvoiceDate"), 4, 2))
      .withColumn("Invoice_YYYY", substring(col("InvoiceDate"), 7, 4))
      .withColumn("Invoice_DD", col("Invoice_DD").cast("Int"))
      .withColumn("Invoice_MM", col("Invoice_MM").cast("Int"))
      .withColumn("Invoice_YYYY", col("Invoice_YYYY").cast("Int"))
      .withColumn("Invoice_DT", to_date(expr("concat(Invoice_DD, '/', Invoice_MM, '/', Invoice_YYYY)"), "d/M/y"))
      .withColumn("Week", weekofyear(col("Invoice_DT"))))

invoice_agg = (cln_invoice_df.selectExpr("Country", "Week", "Quantity", "UnitPrice", "InvoiceNo")
      .groupBy("Country", "Week")
      .agg(
        round(sum(col("Quantity") * col("UnitPrice")), 2).alias("InvoicePrice"),
        sum("Quantity").alias("TotalQuantity"),
        countDistinct("InvoiceNo").alias("NumInvoice"))
      )
invoice_agg.show(truncate = False)

invoice_output = (invoice_agg.coalesce(1)
      .write
      .format("csv")
      .mode("Overwrite")
      .option("header", "true")
      .save("D:/DataSet/OutputDataset/Invoice"))

spark.stop()