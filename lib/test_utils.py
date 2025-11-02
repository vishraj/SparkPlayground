from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_survey_df, count_by_country

class UtilsTest(TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder \
                        .master("local[3]") \
                        .appName("HelloSparkTest") \
                        .getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_datafile_loading(self):
        sample_df = load_survey_df(self.spark, "data/sample.csv")
        result = sample_df.count()
        self.assertEqual(result, 9, "Record count should be 9")

    def test_country_count(self):
        sample_df = load_survey_df(self.spark, "data/sample.csv")
        result_df = count_by_country(sample_df)
        result = result_df.collect()

        count_dict = dict()
        for row in result:
            count_dict[row['Country']] = row['count']

        self.assertEqual(count_dict.get('United States'), 4, "Count for United States should be 4")
        self.assertEqual(count_dict.get('Canada'), 2, "Count for Canada should be 2")
