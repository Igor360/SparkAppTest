from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import time

PATH = 'data/*.csv'

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Lab4") \
    .getOrCreate()

df = spark.read.csv(PATH, header=True) \
    .select('LocalTime', 'Ch', 'T') \
    .cache()

start_time = time.time()

df2 = df.withColumn("temp", F.col("T").cast("float")) \
    .withColumn('Timestamp', F.to_timestamp("LocalTime", "dd.MM.yyyy HH:mm")) \
    .groupBy(F.month(F.col("Timestamp")).alias("month")).agg(F.countDistinct("Timestamp").alias("dates"), F.sum("temp").alias("SUM")) \
    .withColumn("mediumTemp",  (F.col("SUM") / F.col("dates"))) \
    .sort(F.col("month").desc())

df2.show()
df2.printSchema()
print("--- %s seconds ---" % (time.time() - start_time))
