import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, min, max, mean, stddev, skewness, kurtosis, expr, regexp_replace, trim
from pyspark.sql.types import IntegerType, FloatType, DoubleType, LongType, StringType, TimestampType, DateType

# Initialize Spark session
spark = SparkSession.builder.appName("DataQualityCheck").getOrCreate()

def data_quality_report(df, metrics, range_checks, custom_aggregations):
    overall_report = []
    distribution_report = []

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

            overall_report.append({'column': column, **column_metrics})

        elif isinstance(col_type, StringType):
            # Replace null characters and trim whitespace
            df = df.withColumn(column, trim(regexp_replace(col(column), '\x00', '')))

            stats = df.select(
                count(col(column)).alias('count'),
                count(expr(f"{column} is null or {column} = ''")).alias('missing'),
                countDistinct(col(column)).alias('cardinality')
            ).first()

            value_counts = df.groupBy(col(column)).count().orderBy('count', ascending=False).limit(10).collect()
            distribution = [{column: row[column], 'count': row['count']} for row in value_counts]

            column_metrics.update({
                'type': 'string',
                'count': stats['count'],
                'missing': stats['missing'],
                'cardinality': stats['cardinality'],
            })

            overall_report.append({'column': column, **column_metrics})

            for dist in distribution:
                distribution_report.append({'column': column, 'value': dist[column], 'value_count': dist['count']})

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

            overall_report.append({'column': column, **column_metrics})

        for agg_col, agg_funcs in custom_aggregations.get(column, {}).items():
            for agg_name, agg_func in agg_funcs.items():
                custom_agg_result = df.agg(expr(f"{agg_func}({agg_col})").alias(f"{agg_name}")).first()
                column_metrics[f"custom_aggregation_{agg_name}"] = custom_agg_result[f"{agg_name}"]

    return overall_report, distribution_report

def process_files(config_file):
    with open(config_file, 'r') as f:
        config = json.load(f)
    
    metrics = config.get('metrics', {})
    range_checks = config.get('range_checks', {})
    custom_aggregations = metrics.get('custom_aggregations', {})
    output_path = config.get('output_path', '.')

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

        overall_report, distribution_report = data_quality_report(df, metrics, range_checks, custom_aggregations)
        
        # Create DataFrames from the reports
        overall_report_df = pd.DataFrame(overall_report)
        distribution_report_df = pd.DataFrame(distribution_report)

        # Write the reports to CSV files
        overall_output_path = f"{output_path}/{file_path.split('/')[-1].split('.')[0]}_overall_report.csv"
        distribution_output_path = f"{output_path}/{file_path.split('/')[-1].split('.')[0]}_distribution_report.csv"

        overall_report_df.to_csv(overall_output_path, index=False)
        distribution_report_df.to_csv(distribution_output_path, index=False)

        print(f"Reports written for {file_path}")

# Example usage
config_file = "path_to_config.json"
process_files(config_file)
