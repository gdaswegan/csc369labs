# Griffin Aswegan (gaswegan) && Steven Bradley (stbradle)
# CSC 369
# March 18, 2019

import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.types import DoubleType
spark = SparkSession.builder.getOrCreate();

CID = 0
LASTNAME = 1
FIRSTNAME = 2

GID = 0
FLAVOR = 1
FOOD = 2
PRICE = 3

RNUMBER = 0
DATE = 1
RCID = 2

ORDINAL = 1
ITEM = 2

filepath = {}
filepath['customers'] = {}
filepath['customers']['path'] = "/data/BAKERY/customers.csv"
filepath['customers']['schema'] = StructType([
   StructField("Id", StringType(), True),
   StructField("LastName", StringType(), True),
   StructField("FirstName", StringType(), True)
])
filepath['goods'] = {}
filepath['goods']['path'] = "/data/BAKERY/goods.csv"
filepath['goods']['schema'] = StructType([
   StructField("Id", StringType(), True),
   StructField("Flavor", StringType(), True),
   StructField("Food", StringType(), True),
   StructField("Price", StringType(), True)
])
filepath['reciepts'] = {}
filepath['reciepts']['path'] = "/data/BAKERY/reciepts.csv"
filepath['reciepts']['schema'] = StructType([
   StructField("RecieptNumber", StringType(), True),
   StructField("Date", StringType(), True),
   StructField("CustomerId", StringType(), True)
])
filepath['items'] = {}
filepath['items']['path'] = "/data/BAKERY/items.csv"
filepath['items']['schema'] = StructType([
   StructField("RecieptNumber", StringType(), True),
   StructField("Ordinal", StringType(), True),
   StructField("Item", StringType(), True)
])

def cleanCustomers(customer):
   rNumber = customer[CID]
   lastName = customer[LASTNAME]
   firstName = customer[FIRSTNAME]
   if rNumber[0] == ' ':
      rNumber = rNumber[1:]
   rNumber = int(rNumber)
   lNameBLoc = lastName.index("'")
   lNameLLoc = lastName.rindex("'")
   lastName = lastName[lNameBLoc + 1 : lNameLLoc]
   fNameBLoc = firstName.index("'")
   fNameLLoc = firstName.rindex("'")
   firstName = firstName[fNameBLoc + 1 : fNameLLoc]
   return [rNumber, lastName, firstName]

def cleanGoods(good):
   goodId = good[GID]
   flavor = good[FLAVOR]
   food = good[FOOD]
   price = good[PRICE]
   fLoc = flavor.index("'")
   bLoc = flavor.rindex("'")
   flavor = flavor[fLoc + 1 : bLoc]
   fLoc = food.index("'")
   bLoc = food.rindex("'")
   food = food[fLoc + 1 : bLoc]
   fLoc = goodId.index("'")
   bLoc = goodId.rindex("'")
   goodId = goodId[fLoc + 1 : bLoc]
   price = float(price)
   return [goodId, flavor, food, price]

def cleanReciepts(reciept):
   rNumber = reciept[RNUMBER]
   date = reciept[DATE]
   cid = reciept[RCID]
   if rNumber[0] == ' ':
      rNumber = rNumber[1:]
   rNumber = int(rNumber)
   dateBLoc = date.index("'")
   dateELoc = date.rindex("'")
   date = date[dateBLoc + 1:dateELoc]
   if cid[0] == ' ':
      cid = cid[1:]
   cid = int(cid)
   return [rNumber, date, cid]

def cleanItems(item):
   rNumber = item[RNUMBER]
   ordinal = item[ORDINAL]
   itemid = item[ITEM]

   if rNumber[0] == ' ':
      rNumber = rNumber[1:]
   rNumber = int(rNumber)
   if ordinal[0] == ' ':
      ordinal = ordinal[1:]
   ordinal = int(ordinal)
   itemBLoc = itemid.index("'")
   itemELoc = itemid.rindex("'")
   itemid = itemid[itemBLoc + 1:itemELoc]
   return [rNumber, ordinal, itemid]

customersFrame = spark.read.format("csv")\
                  .schema(filepath['customers']['schema'])\
                  .options(set=",", header=True)\
                  .load(filepath['customers']['path'])
goodsFrame = spark.read.format("csv")\
                  .schema(filepath['goods']['schema'])\
                  .options(set=",", header=True)\
                  .load(filepath['goods']['path'])
recieptsFrame = spark.read.format("csv")\
                  .schema(filepath['reciepts']['schema'])\
                  .options(set=",", header=True)\
                  .load(filepath['reciepts']['path'])
itemsFrame = spark.read.format("csv")\
                  .schema(filepath['items']['schema'])\
                  .options(set=",", header=True)\
                  .load(filepath['items']['path'])
customersRdd = customersFrame.rdd.map(list).map(cleanCustomers)
goodsRdd = goodsFrame.rdd.map(list).map(cleanGoods)
recieptsRdd = recieptsFrame.rdd.map(list).map(cleanReciepts)
itemsRdd = itemsFrame.rdd.map(list).map(cleanItems)

customersFrame = customersRdd.toDF(["CId", "LastName", "FirstName"])
goodsFrame = goodsRdd.toDF(["GId", "Flavor", "Food", "Price"])
recieptsFrame = recieptsRdd.toDF(["Reciept", "Date", "CId"])
itemsFrame = itemsRdd.toDF(["Reciept", "Ordinal", "GId"])

def rCount(val):
   return datetime.datetime.strptime(val[0], "%d-%b-%Y")
def toString(val):
   return val.strftime("%d-%b-%Y")

def removeDates(val):
   if val.month == 10 and val.year == 2007 and val.day >= 1 and val.day <= 15:
      return True
   return False

joined = customersFrame.join(recieptsFrame, on="CId")
counted = joined.groupBy("Date", "LastName", "FirstName").count()\
           .withColumnRenamed("count", "NumPurchases")
multiplePurchases = counted.filter("NumPurchases > 1")
dates = multiplePurchases.groupBy("Date").count().rdd.map(list).map(rCount)\
         .distinct().filter(removeDates)
output = dates.sortBy(lambda x: x).map(toString)
output.saveAsTextFile("Lab08/query3")
