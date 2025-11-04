from lib.logging_config import *
from pyspark.sql import *

if __name__ == "__main__":
    spark = SparkSession.builder \
                .master("local[3]") \
                .appName("AggDemo") \
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
                .getOrCreate()
    
    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "AggDemo")

    invoice_df = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv("data/invoice.csv")