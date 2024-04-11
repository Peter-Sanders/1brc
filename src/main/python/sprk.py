from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
import pyspark.sql.functions as F

spark = SparkSession.builder.appName('1brc').getOrCreate()

schema = StructType([
    StructField("name", StringType(), True),
    StructField("temp", DoubleType(), True)
    ])

file = "../../../data/measurements.txt"

df = spark.read.csv(file, schema=schema, header=False, sep = ';')
df.createOrReplaceTempView("data")
# df = spark.read.format("csv".option("header",True).option("sep",";").load(file)

# df.show()

res = spark.sql("select name, min(temp) as mn, avg(temp) as av, max(temp) as mx from data group by name")
res.show()
