from pyspark.sql import *
from pyspark.sql.functions import *

from lib.logging_config import setup_logging, get_logger

if __name__ == "__main__":
    spark = SparkSession.builder \
                .master("local[3]") \
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
                .appName("LogFileDemo") \
                .getOrCreate()

    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "LogFileDemo")

    file_df = spark.read.text("data/apache_logs.txt")
    file_df.printSchema()

    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'

    # extract the individual columns from the regex pattern
    logs_df = file_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                             regexp_extract('value', log_reg, 4).alias('date'),
                             regexp_extract('value', log_reg, 6).alias('request'),
                             regexp_extract('value', log_reg, 10).alias('referrer')
                             )
    logs_df.printSchema()

    logs_df.withColumn("referrer", substring_index("referrer", "/", 3)) \
        .groupby("referrer") \
        .count() \
        .show(100, truncate=False)