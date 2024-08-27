from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql import DataFrame
from datetime import timedelta

def create_positive_label_spark(df: DataFrame, look_back: int) -> DataFrame:
    # Adjust alert_date and calculate date_30_b4
    df = df.withColumn('alert_date', F.date_sub(F.col('alert_date'), 1))
    df = df.withColumn('date_30_b4', F.date_sub(F.col('alert_date'), look_back))

    # Create date range for each row
    df = df.withColumn("date_range", F.expr(f"sequence(date_30_b4, dateoffraud, interval 1 day)"))

    # Explode the date range into individual rows
    df = df.withColumn("observation_date", F.explode(F.col("date_range")))

    # Select and rename columns
    df = df.select(
        F.col("observation_date"),
        "cus_idr", "stg_id", "cus_acc_num", "st_cde", "dateoffraud",
        "closure_reason", "case_alert_id", "alert_id"
    )

    # Add the additional columns
    df = df.withColumn("fin_crime_status", F.lit(1))
    df = df.withColumn("look_back", F.lit(look_back))

    # Sort by observation_date
    df = df.orderBy("observation_date")

    print("Completed: create_positive_label_spark")
    return df


import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row
import unittest

class TestPositiveLabel(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Initialize Spark Session
        cls.spark = SparkSession.builder.master("local[*]").appName("Unit Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def test_positive_label(self):
        # Create sample data
        data = {
            "cus_idr": [1, 2],
            "stg_id": [101, 102],
            "cus_acc_num": ["A1", "B1"],
            "st_cde": ["AA", "BB"],
            "dateoffraud": pd.to_datetime(["2023-08-01", "2023-08-10"]),
            "closure_reason": ["reason1", "reason2"],
            "case_alert_id": [201, 202],
            "alert_id": [301, 302],
            "alert_date": pd.to_datetime(["2023-08-01", "2023-08-10"]),
        }
        
        pdf = pd.DataFrame(data)
        sdf = self.spark.createDataFrame([Row(**row) for row in pdf.to_dict(orient="records")])

        look_back = 30

        # Get results from both Pandas and Spark functions
        pandas_result = create_positive_label(pdf, look_back)
        spark_result = create_positive_label_spark(sdf, look_back)

        # Convert Spark result to Pandas for easy comparison
        spark_result_pdf = spark_result.toPandas()

        # Sorting both dataframes for comparison
        pandas_result = pandas_result.sort_values(by=pandas_result.columns.tolist()).reset_index(drop=True)
        spark_result_pdf = spark_result_pdf.sort_values(by=spark_result_pdf.columns.tolist()).reset_index(drop=True)

        # Assert that both dataframes are equal
        pd.testing.assert_frame_equal(pandas_result, spark_result_pdf, check_like=True)


def create_label_genuine_spark(df_nm: DataFrame) -> DataFrame:
    # Explode the date range into individual dates
    df_nm = df_nm.withColumn(
        "date_range",
        F.expr("sequence(to_date(alert_start_date), to_date(alert_end_date), interval 1 day)")
    )
    
    # Explode the array of dates into individual rows
    df_exploded = df_nm.withColumn("observation_date", F.explode("date_range"))
    
    # Select the needed columns and add new ones
    df_labeled = df_exploded.select(
        "observation_date",
        "cus_lid",
        "stg_id",
        "cus_acc_num",
        "stg_id_cde",
        "closure_reason",
        "case_alert_id",
        "alert_id"
    ).withColumn("fin_crime_status", F.lit("g")
    ).withColumn("lookback", F.lit(0))
    
    # Optionally reset the index or sort the data
    return df_labeled


if __name__ == '__main__':
    unittest.main()



