import unittest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import Row

class CreateLabelGenuineSparkTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[2]").appName("Unit Test").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_create_label_genuine_spark(self):
        input_data = [
            Row(alert_start_date="2023-08-01", alert_end_date="2023-08-03", cus_lid="C001", stg_id="S001", 
                cus_acc_num="A001", stg_id_cde="CODE1", closure_reason="Reason1", case_alert_id="Case001", alert_id="Alert001"),
            Row(alert_start_date="2023-08-05", alert_end_date="2023-08-07", cus_lid="C002", stg_id="S002", 
                cus_acc_num="A002", stg_id_cde="CODE2", closure_reason="Reason2", case_alert_id="Case002", alert_id="Alert002")
        ]
        
        expected_data = [
            Row(observation_date="2023-08-01", cus_lid="C001", stg_id="S001", cus_acc_num="A001", stg_id_cde="CODE1", closure_reason="Reason1", case_alert_id="Case001", alert_id="Alert001", fin_crime_status="g", lookback=0),
            Row(observation_date="2023-08-02", cus_lid="C001", stg_id="S001", cus_acc_num="A001", stg_id_cde="CODE1", closure_reason="Reason1", case_alert_id="Case001", alert_id="Alert001", fin_crime_status="g", lookback=0),
            Row(observation_date="2023-08-03", cus_lid="C001", stg_id="S001", cus_acc_num="A001", stg_id_cde="CODE1", closure_reason="Reason1", case_alert_id="Case001", alert_id="Alert001", fin_crime_status="g", lookback=0),
            Row(observation_date="2023-08-05", cus_lid="C002", stg_id="S002", cus_acc_num="A002", stg_id_cde="CODE2", closure_reason="Reason2", case_alert_id="Case002", alert_id="Alert002", fin_crime_status="g", lookback=0),
            Row(observation_date="2023-08-06", cus_lid="C002", stg_id="S002", cus_acc_num="A002", stg_id_cde="CODE2", closure_reason="Reason2", case_alert_id="Case002", alert_id="Alert002", fin_crime_status="g", lookback=0),
            Row(observation_date="2023-08-07", cus_lid="C002", stg_id="S002", cus_acc_num="A002", stg_id_cde="CODE2", closure_reason="Reason2", case_alert_id="Case002", alert_id="Alert002", fin_crime_status="g", lookback=0),
        ]

        # Create DataFrame from input_data
        df_nm = self.spark.createDataFrame(input_data)

        # Call the function
        df_result = create_label_genuine_spark(df_nm)

        # Convert expected_data to DataFrame for comparison
        df_expected = self.spark.createDataFrame(expected_data)

        # Sort the DataFrames for a consistent comparison
        df_result_sorted = df_result.orderBy("observation_date")
        df_expected_sorted = df_expected.orderBy("observation_date")

        # Collect the DataFrames to lists of Rows for comparison
        result_data = df_result_sorted.collect()
        expected_data = df_expected_sorted.collect()

        # Assertion
        self.assertEqual(result_data, expected_data)


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


if __name__ == "__main__":
    unittest.main()
