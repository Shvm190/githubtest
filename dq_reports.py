import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, min, max, mean, stddev, expr
from pyspark.sql.types import IntegerType, FloatType, DoubleType, LongType, StringType, TimestampType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("DataQualityCheck").getOrCreate()

def data_quality_report(df):
    report = {}

    for column in df.columns:
        col_type = df.schema[column].dataType

        if isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType)):
            stats = df.select(
                count(col(column)).alias('count'),
                count(expr(f"{column} is null or {column} = ''")).alias('missing'),
                min(col(column)).alias('min'),
                max(col(column)).alias('max'),
                mean(col(column)).alias('mean'),
                expr(f'percentile_approx({column}, 0.5)').alias('median'),
                stddev(col(column)).alias('std')
            ).first()
            report[column] = {
                'type': 'numeric',
                'count': stats['count'],
                'missing': stats['missing'],
                'min': stats['min'],
                'max': stats['max'],
                'mean': stats['mean'],
                'median': stats['median'],
                'std': stats['std']
            }

        elif isinstance(col_type, StringType):
            stats = df.select(
                count(col(column)).alias('count'),
                count(expr(f"{column} is null or {column} = ''")).alias('missing'),
                countDistinct(col(column)).alias('cardinality')
            ).first()

            value_counts = df.groupBy(col(column)).count().orderBy('count', ascending=False).limit(10).collect()
            distribution = {row[column]: row['count'] for row in value_counts}

            report[column] = {
                'type': 'string',
                'count': stats['count'],
                'missing': stats['missing'],
                'cardinality': stats['cardinality'],
                'distribution': distribution
            }

        elif isinstance(col_type, (TimestampType, DateType)):
            stats = df.select(
                count(col(column)).alias('count'),
                count(expr(f"{column} is null or {column} = ''")).alias('missing'),
                min(col(column)).alias('min'),
                max(col(column)).alias('max')
            ).first()
            report[column] = {
                'type': 'timestamp',
                'count': stats['count'],
                'missing': stats['missing'],
                'min': stats['min'],
                'max': stats['max']
            }
        
        else:
            report[column] = {
                'type': 'unknown',
                'count': df.select(count(col(column))).first()[0],
                'missing': df.select(count(expr(f"{column} is null or {column} = ''"))).first()[0]
            }

    return report

def process_files(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    all_reports = {}
    for file_path in config["files"]:
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        report = data_quality_report(df)
        all_reports[file_path] = report
    
    return all_reports

# Example usage
config_file = "path_to_config.json"
quality_reports = process_files(config_file)

# Print or save the quality reports
import pprint
pprint.pprint(quality_reports)
