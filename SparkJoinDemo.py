from pyspark.sql import *
from pyspark.sql.functions import *
from lib.logging_config import *

if __name__ == "__main__":
    spark = SparkSession.builder \
                .master("local[3]") \
                .appName("SparkJoinDemo") \
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
                .getOrCreate()
    
    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "SparkJoinDemo")

    orders_list = [("01", "02", 350, 1),
                   ("01", "04", 580, 1),
                   ("01", "07", 320, 2),
                   ("02", "03", 450, 1),
                   ("02", "06", 220, 1),
                   ("03", "01", 195, 1),
                   ("04", "09", 270, 3),
                   ("04", "08", 410, 2),
                   ("05", "02", 350, 1)]
    
    orders_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [("01", "Scroll Mouse", 250, 20),
                    ("02", "Optical Mouse", 350, 20),
                    ("03", "Wireless Mouse", 450, 50),
                    ("04", "Wireless Keyboard", 580, 50),
                    ("05", "Standard Keyboard", 360, 10),
                    ("06", "16 GB Flash Storage", 240, 100),
                    ("07", "32 GB Flash Storage", 320, 50),
                    ("08", "64 GB Flash Storage", 430, 25)]
    
    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    join_expr = orders_df.prod_id == product_df.prod_id

    # inner join
    orders_df.join(product_df, join_expr, "inner") \
                .drop(product_df.qty) \
                .select("order_id", "prod_name", "unit_price", "qty") \
                .show()

    # outer join
    orders_df.join(product_df, join_expr, "left") \
                .drop(product_df.qty) \
                .drop(product_df.prod_id) \
                .select("order_id", "prod_id", "prod_name", "list_price", "unit_price", "qty") \
                .withColumn("prod_name", expr("coalesce(prod_name, prod_id)")) \
                .withColumn("list_price", expr("coalesce(list_price, unit_price)")) \
                .sort("order_id") \
                .show()


    