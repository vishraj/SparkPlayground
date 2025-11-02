from configparser import ConfigParser
from pyspark import SparkConf

def load_spark_configs():
    parser = ConfigParser()
    parser.read("spark.conf")
    configs = parser.items("SPARK_APP_CONFIGS")
    conf = SparkConf()
    conf.setAll(configs)

    return conf

def load_survey_df(spark, file_path):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(file_path)

def count_by_country(survey_df):
    return survey_df \
        .where("Age < 40") \
        .select("Age", "Gender", "Country", "State") \
        .groupBy("Country") \
        .count()