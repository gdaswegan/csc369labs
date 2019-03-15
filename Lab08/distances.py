# Griffin Aswegan (gaswegan)
# CSC 369
# March 4, 2019

import math

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import mean as _mean, stddev_pop as _stddev, col
spark = SparkSession.builder.getOrCreate()
sc = SparkContext.getOrCreate()

filename = "/data/winequality-red-fixed.csv"
schema = StructType([
   StructField("fixed acidity", DoubleType(), True),
   StructField("volatile acidity", DoubleType(), True),
   StructField("citric acid", DoubleType(), True),
   StructField("residual sugar", DoubleType(), True),
   StructField("chlorides", DoubleType(), True),
   StructField("free sulfur dioxide", DoubleType(), True),
   StructField("total sulfur dioxide", DoubleType(), True),
   StructField("density", DoubleType(), True),
   StructField("pH", DoubleType(), True),
   StructField("sulfates", DoubleType(), True),
   StructField("alcohol", DoubleType(), True),
   StructField("quality", DoubleType(), True)
])

FIXED_ACIDITY = 0 #"_c0"
VOLATILE_ACIDITY = 1 #"_c1"
CITRIC_ACID = 2 #"_c2"
RESIDUAL_SUGAR = 3 #"_c3"
CHLORIDES = 4 #"_c4"
FREE_SULFUR_DIOXIDE = 5 #"_c5"
TOTAL_SULFUR_DIOXIDE = 6 #"_c6"
DENSITY = 7 #"_c7"
pH = 8 #"_c8"
SULPHATES = 9 #"_c9"
ALCOHOL = 10 #"_c10"
QUALITY = 11 #"_c11"
ID = 12
PAIR = 13
STD_OFFSET = 11

temp = spark.read.format("csv").schema(schema).options(sep=",", header=True).load(filename)
wine = temp.withColumn("id", monotonically_increasing_id())

# d_raw data
def equalID(val):
   if val[ID] == val[PAIR + ID]:
      return False
   return True

def findDistance(val):
   sumVal = 0
   for i in range(PAIR - 3):
      sumVal += (val[i] - val[PAIR + i]) * (val[i] - val[PAIR + i])
   sumVal = math.sqrt(sumVal)
   return [val[ID], val[ID + PAIR], sumVal]

joined = wine.crossJoin(wine)
pairs = joined.rdd.map(list)

d_raw = pairs.filter(equalID).map(findDistance)
d_raw.saveAsTextFile("Lab08/d_raw")


# d_std data
def makeStandard(val):
   global stats_b
   stats = stats_b.value[0]
   newVal = []
   for i in range(PAIR - 2):
      newVal.append( (val[i] - stats[i]) / stats[i + STD_OFFSET] )
   newVal.append(val[QUALITY])
   newVal.append(val[ID])
   return newVal

def addLst(val):
   return val[0] + val[1]

stats = wine.select(
   _mean(col("fixed acidity")).alias("fa_mean"),
   _mean(col("volatile acidity")).alias("va_mean"),
   _mean(col("citric acid")).alias("ca_mean"),
   _mean(col("residual sugar")).alias("rs_mean"),
   _mean(col("chlorides")).alias("c_mean"),
   _mean(col("free sulfur dioxide")).alias("fsd_mean"),
   _mean(col("total sulfur dioxide")).alias("tsd_mean"),
   _mean(col("density")).alias("d_mean"),
   _mean(col("pH")).alias("ph_mean"),
   _mean(col("sulfates")).alias("s_mean"),
   _mean(col("alcohol")).alias("a_mean"),
   _stddev(col("fixed acidity")).alias("fa_std"),
   _stddev(col("volatile acidity")).alias("va_std"),
   _stddev(col("citric acid")).alias("ca_std"),
   _stddev(col("residual sugar")).alias("rs_std"),
   _stddev(col("chlorides")).alias("c_std"),
   _stddev(col("free sulfur dioxide")).alias("fsd_std"),
   _stddev(col("total sulfur dioxide")).alias("tsd_std"),
   _stddev(col("density")).alias("d_std"),
   _stddev(col("pH")).alias("ph_std"),
   _stddev(col("sulfates")).alias("s_std"),
   _stddev(col("alcohol")).alias("a_std")
)

stats = stats.rdd.map(list).collect()
stats_b = sc.broadcast(stats)

wineRdd = wine.rdd.map(list)
standardized = wineRdd.map(makeStandard)

joined = standardized.cartesian(standardized)
pairs = joined.map(list).map(addLst)

d_std = pairs.filter(equalID).map(findDistance).sortBy(lambda x: x[0])
d_std.saveAsTextFile("Lab08/d_std")
