from pyspark.sql import *
from pyspark.sql.types import StructType, StringType, StructField, DateType, IntegerType

from lib.logging_config import *

if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
            .master("local[3]") \
            .appName("SparkSchemaDemo") \
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
            .getOrCreate()

    # setup your Spark application and python logging
    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "SparkSchemaDemo")

    # CSV
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])
    flightTimeCsvDF = spark.read \
                    .format("csv") \
                    .option("header", "true") \
                    .option("dateFormat", "M/d/y") \
                    .option("mode", "FAILFAST") \
                    .schema(flightSchemaStruct) \
                    .load("data/flight-time.csv")
    flightTimeCsvDF.printSchema()
    flightTimeCsvDF.show(5, False)

    # JSON
    flightSchemaDDL = """
        FL_DATE DATE,
        OP_CARRIER STRING,
        OP_CARRIER_FL_NUM INT,
        ORIGIN STRING,
        ORIGIN_CITY_NAME STRING,
        DEST STRING,
        DEST_CITY_NAME STRING,
        CRS_DEP_TIME INT,
        DEP_TIME INT,
        WHEELS_ON INT,
        TAXI_IN INT,
        CRS_ARR_TIME INT,
        ARR_TIME INT,
        CANCELLED INT,
        DISTANCE INT
    """
    flightTimeJsonDF = spark.read \
                        .format("json") \
                        .schema(flightSchemaDDL) \
                        .option("dateFormat", "M/d/y") \
                        .load("data/flight-time.json")
    flightTimeJsonDF.printSchema()
    flightTimeJsonDF.show(5, False)

    # Parquet
    flightTimeParquetDF = spark.read \
                        .format("parquet") \
                        .load("data/flight-time.parquet")
    flightTimeParquetDF.printSchema()
    flightTimeParquetDF.show(5, False)


