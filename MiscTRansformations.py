import pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder \
                .master("local[3]") \
                .appName("MiscTransformationsDemo") \
                .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=log4j2.properties") \
                .getOrCreate()

    data_list = [
        ("Ravi", 28, 1, 2002),
        ("Abdul", 23, 5, 81),
        ("John", 12, 12, 6),
        ("Rosy", 7, 8, 63),
        ("Abdul", 23, 5, 81)
    ]

    raw_df = spark.createDataFrame(data_list).toDF("name", "day", "month", "year").repartition(3)
    raw_df.printSchema()

    # add a monotonically increasing id
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    df1.show()

    # convert 2 letter yy to 4 letter yyyy
    df2 = df1.withColumn("year", expr("""
        case when year < 21 then year + 2000
        when year < 100 then year + 1900
        else year
        end"""))
    df2.show()

    df3 = df2.withColumn("dob", to_date(expr("concat(day, '/', month, '/', year)"), 'd/M/y'))
    df3.show()

    df4 = df3.drop("day", "month", "year") \
                .dropDuplicates(["name", "dob"]) \
                .sort(expr("dob desc"))
    df4.show()