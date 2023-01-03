from pyspark.sql import SparkSession
import pyspark.sql.functions as func

# https://sparkbyexamples.com/pyspark/pyspark-read-csv-file-into-dataframe/
def main():
    spark = SparkSession.builder.appName("MyApp").getOrCreate()

    df = spark.read \
         .option("header", True) \
         .option('inferSchema', True) \
        .csv("data/transactions.csv")
    print("  - Printing schema")
    df.printSchema()
    print("  - Calling show()")
    df.show()
    print("  - Calling groupby('UserId').avg().show()")
    df.groupBy('UserId').avg().show()
    print("  - Calling groupby('UserId').sum().show()")
    df.groupBy('UserId').sum().show()

    # Give alias to column - https://stackoverflow.com/questions/34394745/how-could-i-order-by-sum-within-a-dataframe-in-pyspark
    print("  - Two topmost spenders")
    df \
        .filter(df.Amount < 0) \
        .groupBy('UserId') \
        .agg(func.sum('Amount').alias('AmountSum')) \
        .orderBy(func.col('AmountSum').asc()) \
        .limit(2) \
        .show()
