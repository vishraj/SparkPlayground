from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id

from lib.logging_config import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
        .config("spark.jars.packages", "org.apache.spark:spark-avro_2.13:4.0.0") \
        .appName("DataSinkDemo") \
        .getOrCreate()

    # setup your Spark application and python logging
    root_class = "SparkPlayground"
    app_name = "DataSinkDemo"
    setup_logging()
    logger = get_logger(root_class + "." + app_name)

    # read the parquet file
    flightTimeParquetDF = spark.read \
            .format("parquet") \
            .parquet("data/flight-time.parquet")

    logger.info(f"Num Partitions before: {flightTimeParquetDF.rdd.getNumPartitions()}")
    flightTimeParquetDF.groupBy(spark_partition_id()).count().show()

    # now repartition to 5 partitions
    partitionedDF = flightTimeParquetDF.repartition(5)
    logger.info(f"Num Partitions after: {partitionedDF.rdd.getNumPartitions()}")
    partitionedDF.groupBy(spark_partition_id()).count().show()

    # write the source file as a avro file
    # partitionedDF.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "data/avro/") \
    #     .save()

    flightTimeParquetDF.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "data/json/") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .save()