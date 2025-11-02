from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

from lib.logging_config import setup_logging, get_logger

def to_date_df(df, fmt, fld):
    return df.withColumn(fld, to_date(col(fld), fmt))

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("SparkDataTransformations") \
        .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
        .getOrCreate()

    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "SparkDataTransformations")

    my_schema = StructType([
        StructField("ID", StringType()),
        StructField("EventDate", StringType())
    ])

    my_rows = [Row("123", "04/05/2020"), Row("124", "4/5/2020"), Row("125", "04/5/2020"), Row("126", "4/05/2020")]
    my_df = spark.createDataFrame(my_rows, my_schema)
    my_df = my_df.repartition(2)

    new_df = to_date_df(my_df, "M/d/y", "EventDate")
    logger.info(f"Num partitions: {new_df.rdd.getNumPartitions()}")
    new_df.show()