from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum
from pyspark.sql.functions import col

# spark = SparkSession \
#         .builder \
#         .appName("example-spark") \
#         .config("spark.sql.crossJoin.enabled","true") \
#         .getOrCreate()

schema = StructType([
    StructField('_Id', IntegerType()),
    StructField('_Tags', StringType()),
    StructField('_ViewCount', IntegerType()),
])

sc = SparkContext().getOrCreate()

sqlContext = SQLContext(sc)

df = sqlContext.read.format("com.databricks.spark.xml") \
        .options(rowTag="row") \
        .options(samplingRatio=0.1) \
        .load("hdfs://localhost:9001/so-analysis/post/", schema=schema)


def strip(str):
    if str is None:
        return None
    return str[1:-1]

stripTags = udf(lambda tag: strip(tag), StringType())

df.printSchema()
df = df.withColumn("_Tags", stripTags(df["_Tags"]))
df = df.withColumn("_Tags", explode(split(df["_Tags"], "\\>\\<")))

result = df.select(col("_Tags").alias("Tags"), col("_ViewCount").alias("View Count")).groupBy("Tags").agg(sum("View Count").alias("Total View")).sort(desc("Total View"))

result.show()

#result.write.format("com.databricks.spark.xml").options(rootTag="tags-popularity", rowTag="tags-count").save("hdfs://localhost:9001/so-analysis/output/tags-popularity.xml")
