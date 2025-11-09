from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from lib.logging_config import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ShuffleJoinDemo") \
        .master("local[3]") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
        .config("spark.sql.shuffle.partitions", "3") \
        .getOrCreate()

    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "ShuffleJoinDemo")

    flight_time_df1 = spark.read.json("data/d1")
    flight_time_df2 = spark.read.json("data/d2")

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")

    # dummyt action to review in Spark UI
    join_df.foreach(lambda x: None)
    input("press a key to stop ...")


