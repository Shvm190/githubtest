def test_split_train_test():
    sample_data = spark.createDataFrame([("cust1",), ("cust2",), ("cust3",), ("cust4",)], ["entity_id"])
    train, test = split_train_test(sample_data)
    assert train.count() + test.count() == 4
    assert train.intersect(test).count() == 0

def test_sample_alert_date():
    sample_data = spark.createDataFrame([("cust1", "2024-01-01"), ("cust2", "2024-02-01")], ["entity_id", "first_alert_date"])
    alert_samples = sample_alert_date(sample_data)
    assert alert_samples.count() == 2
