import pytest
from pyspark.sql import SparkSession

# Create a fixture for setting up a Spark session
@pytest.fixture(scope="module")
def spark():
    spark_session = SparkSession.builder.master("local").appName("pytest").getOrCreate()
    yield spark_session
    spark_session.stop()

# Test the train and test split
def test_split_train_test(spark):
    sample_data = spark.createDataFrame([("cust1",), ("cust2",), ("cust3",), ("cust4",)], ["entity_id"])
    
    # Import the split_train_test function from the main code file
    from transaction_alert_sampler import split_train_test
    
    train, test = split_train_test(sample_data)
    
    # Ensure train and test sets have the expected counts and no overlap
    assert train.count() + test.count() == 4
    assert train.intersect(test).count() == 0

# Test sampling of the alert date
def test_sample_alert_date(spark):
    sample_data = spark.createDataFrame([
        ("cust1", "2024-01-01"),
        ("cust2", "2024-02-01")
    ], ["entity_id", "first_alert_date"])
    
    # Import the sample_alert_date function from the main code file
    from transaction_alert_sampler import sample_alert_date
    
    alert_samples = sample_alert_date(sample_data)
    
    # Ensure the correct count of samples and correct sample type
    assert alert_samples.count() == 2
    assert alert_samples.filter(alert_samples.sample_type == "alert_date").count() == 2

# Test sampling between given periods
def test_sample_period(spark):
    sample_data = spark.createDataFrame([
        ("cust1", "2024-01-01", "2024-01-05"),
        ("cust2", "2024-02-01", "2024-02-10")
    ], ["entity_id", "start_date", "end_date"])
    
    # Import the sample_period function from the main code file
    from transaction_alert_sampler import sample_period
    
    pre_alert_samples = sample_period(sample_data, "start_date", "end_date", 0.5, "pre_alert_period")
    
    # Ensure the sampling logic works correctly with the correct count and sample type
    assert pre_alert_samples.filter(pre_alert_samples.sample_type == "pre_alert_period").count() > 0
