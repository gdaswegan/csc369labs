import pyspark
from pyspark.sql import SparkSession
import os.path
from datetime import datetime
spark = SparkSession.builder.getOrCreate()

base_path = '/data/BAKERY/'
customers_path = os.path.join(base_path, 'customers.csv')
receipts_path = os.path.join(base_path, 'reciepts.csv')

customers = spark.read.csv(customers_path, header=True)
receipts = spark.read.csv(receipts_path, header=True)

customers_rdd = customers.rdd
receipts_rdd = receipts.rdd

target_date = datetime(2007, 10, 3, 0, 0)
customer_ids = receipts_rdd \
    .filter(lambda x: datetime.strptime(x[' Date'], " '%d-%b-%Y'") == target_date) \
    .map(lambda x: int(x[' CustomerId'])) \
    .distinct() \
    .collect()

customer_names = customers_rdd \
    .filter(lambda x: int(x.Id) in customer_ids) \
    .sortBy(lambda x: x[' LastName']) \
    .map(lambda x: (x[' FirstName'], x[' LastName']))

customer_names.foreach(print)
