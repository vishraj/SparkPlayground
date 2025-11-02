from pyspark.sql import *

from lib.logging_config import setup_logging, get_logger

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("SparkSQLTableDemo") \
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
            .enableHiveSupport() \
            .getOrCreate()

    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "SparkSQLTableDemo")

    # read the parquet file
    flightTimeParquetDF = spark.read.format("parquet") \
                            .load("data/flight-time.parquet")

    spark.sql("create database if not exists airline_db")
    spark.catalog.setCurrentDatabase("airline_db")

    flightTimeParquetDF.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_time_tbl")

    logger.info(spark.catalog.listTables("airline_db"))
