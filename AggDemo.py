from lib.logging_config import *
from pyspark.sql import *
from pyspark.sql.functions import *

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
                    .csv("data/invoices.csv")
    
    invoice_df.printSchema()
    
    invoice_df.select(count("*").alias("Count *"),
                      sum("Quantity").alias("TotalQuantity"),
                      avg("UnitPrice").alias("AvgPrice"),
                      count_distinct("InvoiceNo").alias("CountDistinct")
                      ).show()
    
    invoice_df.selectExpr("count(1) as `count 1`",
                          "count(StockCode) as `count field`",
                          "sum(Quantity) as TotalQuantity",
                          "avg(UnitPrice) as AvgPrice"
                          ).show()
    
    # SQL method
    logger.info("Using Spark SQL to aggregate data ...")
    invoice_df.createOrReplaceTempView("sales")
    summary_sql = spark.sql("""
                            select Country, InvoiceNo,
                                sum(Quantity) as totalQuantity,
                                round(sum(Quantity * UnitPrice)) as InvoiceValue
                            from sales
                            group by Country, InvoiceNo
                            """)
    summary_sql.show()

    # DF method
    logger.info("Using DataFrames API to aggregate data ...")
    summary_df = invoice_df.groupBy("Country", "InvoiceNo") \
                    .agg(sum("Quantity").alias("TotalQuantity"),
                         expr("round(sum(Quantity * UnitPrice)) as InvoiceValue")
                    )
    summary_df.show()

    # Agg 2 example
    logger.info("Aggregating by Country and Week No ...")

    NumInvoices = count_distinct("InvoiceNo").alias("NumInvoices")
    InvoiceValue = expr("round(sum(Quantity * UnitPrice)) as InvoiceValue")
    TotalQuantity = sum("Quantity").alias("TotalQuantity")

    summary_df2 = invoice_df \
                    .withColumn("InvoiceDate", to_date(col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
                    .where("year(InvoiceDate) == 2010") \
                    .withColumn("WeekNumber", weekofyear(col("InvoiceDate"))) \
                    .groupBy("Country", "WeekNumber") \
                    .agg(NumInvoices, TotalQuantity, InvoiceValue)
    
    # write the output to a parquet file
    summary_df2.coalesce(1) \
        .write \
        .format("parquet") \
        .mode("overwrite") \
        .save("output")
    
    summary_df2.sort("Country", "WeekNumber").show()



        
                    