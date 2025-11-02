import re

from pyspark.sql import *
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType

from lib.logging_config import setup_logging, get_logger

def parse_gender(gender: str) -> str:
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"

if __name__ == "__main__":
    spark = SparkSession.builder \
                .appName("UDF Demo") \
                .master("local[3]") \
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
                .getOrCreate()

    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "UDFDemo")

    survey_df = spark.read \
                    .option("header", "true") \
                    .option("inferSchema", "true") \
                    .csv("data/survey.csv")

    # invoke the UDF
    parse_gender_udf = udf(parse_gender, StringType())
    logger.info("Catalog entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    survey_df2.show(10)

    # invoke UDFs using select col expressions
    spark.udf.register("parse_gender_udf", parse_gender, StringType())
    logger.info("Catalog entry:")
    [logger.info(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf(Gender)"))
    survey_df3.show(10)






