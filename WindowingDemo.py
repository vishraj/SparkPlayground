from pyspark.sql import *
from pyspark.sql.functions import *
from lib.logging_config import *

if __name__ == "__main__":
    spark = SparkSession.builder \
                .master("local[3]") \
                .appName("WindowingDemo") \
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
                .getOrCreate()
    
    setup_logging()
    logger = get_logger("SparkPlayground" + "." + "WindowingDemo")

    logger.info("Calculating running total ...")
    summary_df = spark.read.parquet("output/*.parquet")
    
    # define a window
    running_total_window = Window.partitionBy("Country") \
                                .orderBy("WeekNumber") \
                                .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    # calculate running total
    window_df = summary_df.withColumn("RunningTotal", sum("InvoiceValue").over(running_total_window))
    window_df.show()

