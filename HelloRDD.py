import sys
from collections import namedtuple

from pyspark import SparkConf
from pyspark.sql import SparkSession
from lib.logging_config import setup_logging, get_logger

SurveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
if __name__ == "__main__":
    conf = SparkConf() \
            .setMaster("local[3]") \
            .setAppName("HelloRDD")

    spark = SparkSession.builder \
            .config(conf=conf) \
            .getOrCreate()

    # setup your Spark application and python logging
    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "HelloRDD")

    if len(sys.argv) != 2:
        logger.error("Usage: HelloSpark <filename>")
        sys.exit(-1)

    sc = spark.sparkContext
    linesRDD = sc.textFile(sys.argv[1])
    partitionedRDD = linesRDD.repartition(2)

    colsRDD = partitionedRDD.map(lambda line: line.replace('"', '').split(","))
    selectRDD = colsRDD.map(lambda cols: SurveyRecord(int(cols[1]), cols[2], cols[3], cols[4]))
    filterRDD = selectRDD.filter(lambda record: record.Age < 40)
    kvRDD = filterRDD.map(lambda record: (record.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda a, b: a + b)

    colsList = countRDD.collect()
    for x in colsList:
        logger.info(x)

