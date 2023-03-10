from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import StructType

if __name__ == "__main__":
    print("Invoice Data Analysis 3")

spark = SparkSession.builder.appName("Invoice Data Analysis 3").master("local[3]").getOrCreate()

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
    .withColumn("Invoice_DT", to_date(expr("concat(Invoice_DD, '/', Invoice_MM, '/', Invoice_YYYY)"),"d/M/y"))
    .withColumn("Week", weekofyear(col("Invoice_DT"))))

cln_invoice_df.show(10, truncate = False)

invoice_agg = cln_invoice_df.selectExpr("Country", "Week", "Quantity", "UnitPrice", "InvoiceNo") \
      .groupBy("Country", "Week") \
      .agg(
        sum(col("UnitPrice") * col("Quantity")).alias("InvoiceValue"),
        sum(col("Quantity")).alias("TotalQuantity"),
        count(col("InvoiceNo")).alias("NumInvoices"))

invoice_agg.show(truncate = False)

invoice_win = invoice_agg.selectExpr("Country", "Week", "NumInvoices", "TotalQuantity", "InvoiceValue") \
      .withColumn("RunningTotal", sum("InvoiceValue").over(Window.partitionBy(col("Country")).orderBy(desc("Week")))) \
      .where("Country = 'EIRE' and week in (48, 49, 50, 51)")
invoice_agg.show(truncate = False)

spark.stop()