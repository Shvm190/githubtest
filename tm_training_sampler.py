from pyspark.sql import functions as F
from pyspark.sql import Window

# Global variables for sampling percentages
X = 0.3  # 30% pre-alert period
Y = 0.2  # 20% post-alert period
Z1 = 0.15  # 15% pre-alert genuine period
Z2 = 0.25  # 25% post-review genuine period

def split_train_test(data, train_ratio=0.6):
    """ Splits customers into train and test sets with no overlap. """
    customer_ids = data.select("entity_id").distinct()
    train, test = customer_ids.randomSplit([train_ratio, 1 - train_ratio], seed=42)
    
    # Assign train or test label
    train = train.withColumn("train_test", F.lit("train"))
    test = test.withColumn("train_test", F.lit("test"))
    
    return train, test

def sample_alert_date(data):
    """ Sample the alert date (first_alert_date) for all records. """
    return data.select("entity_id", "first_alert_date") \
               .withColumnRenamed("first_alert_date", "date_snapshot") \
               .withColumn("sample_type", F.lit("alert_date"))

def sample_period(data, start_col, end_col, percentage, sample_type):
    """ Samples dates between start_col and end_col based on the provided percentage. """
    date_range = F.expr(f"sequence({start_col}, {end_col}, interval 1 day)")
    sampled_dates = data.withColumn("date_array", date_range) \
                        .withColumn("date_sample", F.expr(f"slice(date_array, 1, cast(size(date_array) * {percentage} as int))")) \
                        .select("entity_id", F.explode("date_sample").alias("date_snapshot")) \
                        .withColumn("sample_type", F.lit(sample_type))
    return sampled_dates

def filter_genuine_periods(data, customer_window):
    """ Filters the genuine periods before and after suspicious windows for each customer. """
    # Filter pre-alert genuine period (dates before suspicion_start_date)
    pre_alert_genuine = data.withColumn("pre_alert_genuine_start", F.date_sub("suspicion_start_date", 365)) \
                            .filter(~(F.col("date_snapshot").between(F.col("suspicion_start_date"), F.col("suspicion_confirmed_date")))) \
                            .select("entity_id", "pre_alert_genuine_start", "suspicion_start_date")
    
    # Similarly for post-review genuine period after suspicion_confirmed_date
    post_review_genuine = data.withColumn("post_review_genuine_end", F.date_add("suspicion_confirmed_date", 365)) \
                              .filter(~(F.col("date_snapshot").between(F.col("suspicion_start_date"), F.col("suspicion_confirmed_date")))) \
                              .select("entity_id", "post_review_genuine_end", "suspicion_confirmed_date")
    
    return pre_alert_genuine, post_review_genuine

def build_final_dataset(data):
    """ Main function to sample all periods and build final dataset. """
    train, test = split_train_test(data)
    
    # Sample alert date
    alert_samples = sample_alert_date(data)
    
    # Sample pre-alert, post-alert, pre-genuine, and post-review periods
    pre_alert_samples = sample_period(data, "suspicion_start_date", "first_alert_date", X, "pre_alert_period")
    post_alert_samples = sample_period(data, "first_alert_date", "suspicion_confirmed_date", Y, "post_alert_period")
    
    # Pre and post genuine periods filtering
    pre_alert_genuine, post_review_genuine = filter_genuine_periods(data, train)
    pre_alert_genuine_samples = sample_period(pre_alert_genuine, "pre_alert_genuine_start", "suspicion_start_date", Z1, "pre_alert_genuine_period")
    post_review_genuine_samples = sample_period(post_review_genuine, "suspicion_confirmed_date", "post_review_genuine_end", Z2, "post_review_genuine_period")
    
    # Combine all samples
    final_data = alert_samples.union(pre_alert_samples).union(post_alert_samples) \
                             .union(pre_alert_genuine_samples).union(post_review_genuine_samples)
    
    return final_data

# Example data loading (assume data is loaded into a PySpark DataFrame 'df')
# df = spark.read.csv("path_to_data")

# Build final sampled dataset
final_sampled_data = build_final_dataset(df)
final_sampled_data.show()
