from pyspark.sql import SparkSession
import pyspark.sql.functions as func

def main():
    spark = SparkSession.builder \
            .config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.1")\
            .appName("MyApp").getOrCreate()

    df = spark.read \
        .format('avro') \
        .load("data/transactions.avro")

    print("  - Printing schema")
    df.printSchema()

    print("  - Calling show()")
    df.show()

    print("  - Two topmost spenders")
    df \
        .filter(df.amount < 0) \
        .groupBy('userid') \
        .agg(func.sum('amount').alias('amount_sum')) \
        .orderBy(func.col('amount_sum').asc()) \
        .limit(2) \
        .show()
