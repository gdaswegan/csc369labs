import pyspark
from pyspark.sql import SparkSession
import os.path
from datetime import datetime
spark = SparkSession.builder.getOrCreate()

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

base_path = '/data/BAKERY/'
customers_path = os.path.join(base_path, 'customers.csv')
goods_path = os.path.join(base_path, 'goods.csv')
receipts_path = os.path.join(base_path, 'reciepts.csv')
items_path = os.path.join(base_path, 'items.csv')


def clean_column_names(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, column.strip().replace('\'', ''))
    return df


customers = clean_column_names(spark.read.csv(customers_path, header=True))
goods = clean_column_names(spark.read.csv(goods_path, header=True))
receipts = clean_column_names(spark.read.csv(receipts_path, header=True))
items = clean_column_names(spark.read.csv(items_path, header=True))

customers_rdd = customers.rdd
goods_rdd = goods.rdd
receipts_rdd = receipts.rdd
items_rdd = items.rdd


def clean_goods(good):
    goodId = good[GID]
    flavor = good[FLAVOR]
    food = good[FOOD]
    price = good[PRICE]
    fLoc = flavor.index("'")
    bLoc = flavor.rindex("'")
    flavor = flavor[fLoc + 1: bLoc]
    fLoc = food.index("'")
    bLoc = food.rindex("'")
    food = food[fLoc + 1 : bLoc]
    fLoc = goodId.index("'")
    bLoc = goodId.rindex("'")
    goodId = goodId[fLoc + 1 : bLoc]
    price = float(price)
    return [goodId, flavor, food, price]


def clean_reciepts(reciept):
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


def clean_items(item):
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


def filter_dates(x, lower, upper):
    target = datetime.strptime(x['Date'], '%d-%b-%Y')
    return (target >= lower) and (target <= upper)


min_date = datetime(2007, 10, 1)
max_date = datetime(2007, 10, 31)

cleaned_goods = goods_rdd.map(list).map(clean_goods).toDF(['Item', 'Flavor', 'Food', 'Price'])
cleaned_receipts = receipts_rdd.map(list).map(clean_reciepts).toDF(['Reciept', 'Date', 'CustomerId'])
cleaned_items = items_rdd.map(list).map(clean_items).toDF(['Reciept', 'Ordinal', 'Item'])

receipts_rdd = cleaned_receipts.rdd

october_purchases = receipts_rdd \
    .filter(lambda x: filter_dates(x, min_date, max_date)) \
    .toDF() \
    .withColumnRenamed('RecieptNumber', 'Reciept') \
    .drop('RecieptNumber')

joined_df = october_purchases \
    .join(other=cleaned_items, on='Reciept') \
    .join(other=cleaned_goods, on='Item')

cookies = joined_df.filter(joined_df['Food'] == 'Cookie')

total_price = cookies.select('Price').rdd.map(lambda x: x.Price).reduce(lambda x, y: x + y)

print(total_price)
