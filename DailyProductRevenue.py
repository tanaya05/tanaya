from pyspark.sql import SparkSession
spark =SparkSession.builder.appName("DPR").getOrCreate()

orders = spark.read. \
  csv("s3://myemrhivescriptbucket-ravi/retail_db/orders",
     schema="order_id INT, order_date TIMESTAMP, order_customer_id INT, order_status STRING")
orderItems = spark.read. \
  csv("s3://myemrhivescriptbucket-ravi/retail_db/order_items",
     schema='''order_item_id INT,
      order_item_order_id INT,
      order_item_product_id INT,
      order_item_quantity INT,
      order_item_subtotal FLOAT,
      order_item_product_price FLOAT''')

products = spark.read. \
  csv("s3://myemrhivescriptbucket-ravi/retail_db/products",
     schema='''product_id INT,
      product_category_id INT,
      product_name STRING,
      product_description STRING,
      product_price FLOAT,
      product_image STRING''')

ordersCompleted = orders.filter("order_status in ('COMPLETE','CLOSED')")

from pyspark.sql.functions import col

joinResults = ordersCompleted.join(orderItems, col("order_id") == orderItems["order_item_order_id"]). \
  join(products, col("product_id") == col("order_item_product_id")). \
  select("order_date", "product_name", "order_item_subtotal")


from pyspark.sql.functions import sum, round

dailyProductRevenue = joinResults. \
  groupBy("order_date", "product_name"). \
  agg(round(sum("order_item_subtotal"), 2).alias("revenue"))


dailyProductRevenueSorted = dailyProductRevenue. \
  orderBy("order_date", col("revenue").desc())

spark.conf.set("spark.sql.shuffle.partitions",1)
dailyProductRevenueSorted.write.mode("overwrite").csv("s3://myemrhivescriptbucket-ravi/retail_db/daily_product_revenue1")
