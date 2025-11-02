import sys

from pyspark.sql import SparkSession

from lib.logging_config import setup_logging, get_logger

if __name__ == "__main__":
    spark = SparkSession \
                .builder \
                .master("local[3]") \
                .appName("HelloSparkSQL") \
                .getOrCreate()

    # setup your Spark application and python logging
    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "HelloSparkSQL")

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSparkSQL <filename>")
        sys.exit(-1)

    surveyDF = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(sys.argv[1])

    surveyDF.createOrReplaceTempView("survey_tbl")
    countDF = spark.sql("SELECT Country, count(1) as count FROM survey_tbl WHERE Age < 40 GROUP BY Country")
    countDF.show()
