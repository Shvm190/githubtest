"""

Approach:

# Approach:

Splitting Train and Test Data:
- Use a random split to ensure 60:40 ratio between train and test datasets.
- Ensure no customer appears in both datasets.

Sampling Based on Suspicion Periods:
- Sample based on the different periods:
    - Alert Date: Always include the first alert date for each customer entry.
    - Pre-Alert Period: Retain X% of the dates between the suspicion start and first alert date.
    - Post-Alert Period: Retain Y% of the dates between the first alert and suspicion confirmed date.
    - Pre-Alert Genuine Period: Retain Z1% of the dates before the suspicion start date, excluding other suspicious windows for the same customer.
    - Post-Review Genuine Period: Retain Z2% of the dates after suspicion confirmed date, excluding other suspicious windows for the same customer.

Final Schema:
- Columns:
    - entity_id: Customer ID.
    - date_snapshot: The sampled date.
    - sample_type: Describes whether it's an "alert date", "pre-alert", "post-alert", etc.
    - train_test: Identifies whether the customer belongs to train or test data.


Pre-Requisite:

In addition to the environment setup (Python, PySpark, etc.), your main PySpark code will need a dataset with the following columns, as per the problem description. Here are the specific details:

Columns Required:
entity_id: String type, representing customer IDs.
suspicion_start_date: Date type, the start date of the suspicion period.
first_alert_date: Date type, the date of the first transaction alert.
suspicion_confirmed_date: Date type, the date suspicion is confirmed.
Your data may look like the following in PySpark:

entity_id	suspicion_start_date	first_alert_date	suspicion_confirmed_date
cust1	2024-01-01	2024-01-05	2024-01-10
cust2	2024-02-01	2024-02-10	2024-02-15
Date Formats: Make sure dates are in the correct format (yyyy-MM-dd) or converted to a PySpark DateType.
Multiple Entries for One Customer: The dataset should account for customers having multiple entries, as per your business logic (where one customer might have two separate entries without overlapping dates).



Sample data creation:

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql import functions as F

# Sample data schema
schema = StructType([
    StructField("entity_id", StringType(), True),
    StructField("suspicion_start_date", DateType(), True),
    StructField("first_alert_date", DateType(), True),
    StructField("suspicion_confirmed_date", DateType(), True)
])

# Example data
data = [
    ("cust1", "2024-01-01", "2024-01-05", "2024-01-10"),
    ("cust2", "2024-02-01", "2024-02-10", "2024-02-15"),
    ("cust1", "2024-03-01", "2024-03-10", "2024-03-15"),  # Multiple entry for cust1
]

# Initialize Spark session and create DataFrame
spark = SparkSession.builder.appName("test").getOrCreate()

df = spark.createDataFrame(data, schema)

# To ensure date format is correct, cast string dates to DateType
df = df.withColumn("suspicion_start_date", F.to_date(F.col("suspicion_start_date"))) \
       .withColumn("first_alert_date", F.to_date(F.col("first_alert_date"))) \
       .withColumn("suspicion_confirmed_date", F.to_date(F.col("suspicion_confirmed_date")))

df.show()

Author: Shivam Singh
"""

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
