from pyspark.sql import *
from pyspark.sql.functions import *
from lib.logging_config import *

if __name__ == "__main__":
    spark = SparkSession.builder \
                .master("local[3]") \
                .appName("BucketJoinDemo") \
                .enableHiveSupport() \
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
                .getOrCreate()
    
    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "BucketJoinDemo")

    df1 = spark.read.json("data/d1")
    df2 = spark.read.json("data/d2")

    '''
    # bucket the dataframes
    spark.sql("CREATE DATABASE IF NOT EXISTS spark_playground")
    spark.sql("USE spark_playground")

    df1.coalesce(1).write \
    .bucketBy(3, "id") \
    .mode("overwrite") \
    .saveAsTable("spark_playground.flight_data1")

    df2.coalesce(1).write \
    .bucketBy(3, "id") \
    .mode("overwrite") \
    .saveAsTable("spark_playground.flight_data2")
    '''

    # after bucketing, do an inner join
    df3 = spark.read.table("spark_playground.flight_data1")
    df4 = spark.read.table("spark_playground.flight_data2")

    # disable broadcast join
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

    join_expr = df3.id == df4.id
    join_df = df3.join(df4, join_expr, "inner")

    # dummyt action to review in Spark UI
    join_df.collect()
    input("press a key to stop ...")
