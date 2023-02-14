from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == "__main__":
    print("Aggregation on Churn Modeling 10")

spark = SparkSession.builder.appName("Aggregation on Churn Modeling 10").master("local[3]").getOrCreate()

churn_df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(r"D:\DataSet\DataSet\SparkDataSet\ChurnModeling.csv")

churn_sel = churn_df.selectExpr("CustomerID as CustID", "Geography as Country", "Gender as Sex",
                                "Age", "Tenure", "Balance", "HasCrCard as Credit_Card",
                                "IsActiveMember as Active", "EstimatedSalary as Salary") \
                    .filter("Country in ('France', 'Spain')") \
                    .filter(col("Age") >= 18) \
                    .filter(col("Active") == 1) \
                    .where("Balance > 0") \
                    .orderBy(col("CustID").asc())

churn_sel.show(10, False)
print(f"Records Effected : {churn_sel.count()}")

# churn_df.show(10, truncate=False)
# print(f"Records Effected {churn_df.count()}")