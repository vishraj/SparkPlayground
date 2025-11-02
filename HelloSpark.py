import sys

from pyspark.sql import *
from lib.logging_config import *
from lib.utils import *

if __name__ == "__main__":
    # load the spark configurations
    conf = load_spark_configs()

    # Initialize Spark session
    spark = SparkSession.builder \
        .config(conf=conf) \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
        .getOrCreate()

    root_class = "SparkPlayground"
    app_name = conf.get("spark.app.name")

    # setup your Spark application and python logging
    setup_logging()
    logger = get_logger(root_class + "." + app_name)

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    logger.info("Spark Application Started")
    survey_df = load_survey_df(spark, sys.argv[1])

    partitioned_survey_df = survey_df.repartition(2)
    count_df = count_by_country(partitioned_survey_df)
    logger.info(count_df.collect())

    spark.stop()
    logger.info("Spark Application Ended")



