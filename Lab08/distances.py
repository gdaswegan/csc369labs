# Griffin Aswegan (gaswegan)
# CSC 369
# March 4, 2019

import sys
from pprint import pprint as pretty

from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import monotonically_increasing_id
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

ROW = 0
PAIRED = 0
DIFF = 1

def reKey(val):
   return (val[ID], val[QUALITY])

def fixData(val):
   string = val[1]
   string = string[string.index("[") + 1 : string.index("]")]
   strings = string.split(",")
   thisId = int(strings[0])
   paired = int(strings[1])
   distance = float(strings[2])
   return (thisId, [paired, distance])

def getNearest(val):
   global k_b
   k = k_b.value
   largest = []
   for pair in val[1]:
      if (len(largest) < k):
         largest.append(pair)
      else:
         maxPair = 0
         for compare in range(len(largest)):
            if (largest[compare][DIFF] > largest[maxPair][DIFF]):
               maxPair = compare
         if pair[DIFF] < largest[maxPair][DIFF]:
            largest[maxPair][PAIRED] = pair[PAIRED]
            largest[maxPair][DIFF] = pair[DIFF]
   
   rows = []
   for item in largest:
      rows.append(item[PAIRED])
   return (val[0], rows)

temp = spark.read.format("csv").schema(schema).options(sep=",", header=True).load(filename)
wine = temp.withColumn("id", monotonically_increasing_id()).rdd.map(list).map(reKey)

k = int(sys.argv[1])
temp = sc.newAPIHadoopFile(sys.argv[2], "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                           "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text")
distances = temp.map(fixData).groupByKey()
k_b = sc.broadcast(k)
nearest = distances.map(getNearest)

topKAndQuality = nearest.join(wine).sortBy(lambda x : x[0])

wineList = topKAndQuality.collect() #format [(id, ([topk], quality))...]

confusion = [[0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], 
             [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0], [0, 0, 0, 0, 0, 0]]
base = 3

output = []
correct = 0
for row in wineList:
   qualities = {'3.0': 0, '4.0': 0, '5.0': 0, '6.0': 0, '7.0': 0, '8.0': 0}
   for paired in row[1][0]:
      qualities[str(wineList[paired][1][1])] += 1
   maxKey = "3.0"
   for key in qualities:
      if qualities[maxKey] < qualities[key]:
         maxKey = key
   quality = int(float(maxKey))
   pretty(quality)
   if quality == int(row[1][1]):
      correct += 1
   confusion[quality - base][int(row[1][1]) - base] += 1
   output.append([row[0], quality, row[1][1]])

pretty(float(correct)/len(wineList))
pretty(confusion)
pretty(output)
#rddOut = sc.parallelize(output)
#rddOut.saveAsTextFile("Lab08/knn")
