from pyspark.sql import SparkSession
import os.path
spark = SparkSession.builder.getOrCreate()

base_path = '/data/BAKERY/'
goods_path = os.path.join(base_path, 'goods.csv')

goods = spark.read.csv(goods_path, header=True)

goods_rdd = goods.rdd


cheap_chocolates = goods_rdd.filter(lambda x: (x.Flavor == "'Chocolate'") and (float(x.Price) < 5))
final_rdd = cheap_chocolates.sortBy(lambda x: x.Price, ascending=False).map(lambda x: (x.Flavor, x.Food, x.Price))

final_rdd.foreach(print)
