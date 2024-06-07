import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, min, max, mean, stddev, skewness, kurtosis, expr
from pyspark.sql.types import IntegerType, FloatType, DoubleType, LongType, StringType, TimestampType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("DataQualityCheck").getOrCreate()

def data_quality_report(df, metrics, range_checks, custom_aggregations):
    report = []

    for column in df.columns:
        col_type = df.schema[column].dataType
        column_metrics = {}

        if isinstance(col_type, (IntegerType, FloatType, DoubleType, LongType)):
            stats = df.select(
                count(col(column)).alias('count'),
                count(expr(f"{column} is null or {column} = ''")).alias('missing'),
                min(col(column)).alias('min'),
                max(col(column)).alias('max'),
                mean(col(column)).alias('mean'),
                expr(f'percentile_approx({column}, 0.5)').alias('median'),
                stddev(col(column)).alias('std'),
                skewness(col(column)).alias('skewness'),
                kurtosis(col(column)).alias('kurtosis')
            ).first()
            
            column_metrics.update({
                'type': 'numeric',
                'count': stats['count'],
                'missing': stats['missing'],
                'min': stats['min'],
                'max': stats['max'],
                'mean': stats['mean'],
                'median': stats['median'],
                'std': stats['std'],
                'skewness': stats['skewness'],
                'kurtosis': stats['kurtosis']
            })

            if column in range_checks.get('numeric', {}):
                expected_min = range_checks['numeric'][column].get('min')
                expected_max = range_checks['numeric'][column].get('max')
                column_metrics['range_check'] = {
                    'expected_min': expected_min,
                    'expected_max': expected_max,
                    'min_check': stats['min'] >= expected_min if expected_min is not None else 'N/A',
                    'max_check': stats['max'] <= expected_max if expected_max is not None else 'N/A'
                }

        elif isinstance(col_type, StringType):
            stats = df.select(
                count(col(column)).alias('count'),
                count(expr(f"{column} is null or {column} = ''")).alias('missing'),
                countDistinct(col(column)).alias('cardinality')
            ).first()

            value_counts = df.groupBy(col(column)).count().orderBy('count', ascending=False).limit(10).collect()
            distribution = {row[column]: row['count'] for row in value_counts}

            unique_values = df.select(col(column)).distinct().limit(10).rdd.flatMap(lambda x: x).collect()

            column_metrics.update({
                'type': 'string',
                'count': stats['count'],
                'missing': stats['missing'],
                'cardinality': stats['cardinality'],
                'distribution': distribution,
                'unique_values': unique_values
            })

        elif isinstance(col_type, (TimestampType, DateType)):
            stats = df.select(
                count(col(column)).alias('count'),
                count(expr(f"{column} is null or {column} = ''")).alias('missing'),
                min(col(column)).alias('min'),
                max(col(column)).alias('max')
            ).first()
            
            column_metrics.update({
                'type': 'timestamp',
                'count': stats['count'],
                'missing': stats['missing'],
                'min': stats['min'],
                'max': stats['max']
            })

            if column in range_checks.get('timestamp', {}):
                expected_min = range_checks['timestamp'][column].get('min')
                expected_max = range_checks['timestamp'][column].get('max')
                column_metrics['range_check'] = {
                    'expected_min': expected_min,
                    'expected_max': expected_max,
                    'min_check': stats['min'] >= expected_min if expected_min is not None else 'N/A',
                    'max_check': stats['max'] <= expected_max if expected_max is not None else 'N/A'
                }

        for agg_col, agg_funcs in custom_aggregations.get(column, {}).items():
            for agg_name, agg_func in agg_funcs.items():
                custom_agg_result = df.agg(expr(f"{agg_func}({agg_col})").alias(f"{agg_name}")).first()
                column_metrics[f"custom_aggregation_{agg_name}"] = custom_agg_result[f"{agg_name}"]

        report.append({'column': column, **column_metrics})

    return report

def process_files(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    metrics = config.get('metrics', {})
    range_checks = config.get('range_checks', {})
    custom_aggregations = metrics.get('custom_aggregations', {})

    all_reports = []
    for file_info in config["files"]:
        file_path = file_info["path"]
        file_format = file_info["format"]
        options = file_info.get("options", {})

        if file_format == "csv":
            df = spark.read.csv(file_path, **options)
        elif file_format == "json":
            df = spark.read.json(file_path, **options)
        elif file_format == "parquet":
            df = spark.read.parquet(file_path, **options)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        report = data_quality_report(df, metrics, range_checks, custom_aggregations)
        for r in report:
            r['file'] = file_path
        all_reports.extend(report)
    
    return pd.DataFrame(all_reports)

def write_reports_to_parquet(df, output_path):
    df.to_parquet(output_path, index=False)

# Example usage
config_file = "path_to_config.json"
quality_reports_df = process_files(config_file)

# Save the quality reports to a Parquet file
output_path = quality_reports_df.loc[0, "output_path"]
write_reports_to_parquet(quality_reports_df, output_path)

# To print a sample of the dataframe
print(quality_reports_df.head())
