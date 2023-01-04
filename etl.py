import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import udf, col, monotonically_increasing_id, lit, length


class DATA_PATH:
    APPSTORE_APP = 'appstore_app'
    PLAYSTORE_APP = 'playstore_app.parquet'
    DEVELOPER = 'developer.parquet'
    APP = 'app.parquet'
    TIME = 'time.parquet'
    GREAT_APP = 'great_app.parquet'


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def read_appstore_data(spark: SparkSession, input_path: str):
    '''
    Load data from `input_path` into DataFrame
    
    Args:
        spark: spark.sql.SparkSession 
            Spark session.
        input_path: string
            Data in csv format path.
            
    Returns:
        appstore_apps_df: spark.sql.DataFrame
            DataFrame that load from `input_path`
    '''
    
    appstore_apps_schema = StructType([
        StructField('app_id', StringType(), True),
        StructField('app_name', StringType(), True),
        StructField('appstore_url', StringType(), True),
        StructField('genre', StringType(), True),
        StructField('content_rating', StringType(), True),
        StructField('size', StringType(), True),
        StructField('min_ios_version', StringType(), True),
        StructField('released_ts', StringType(), True),
        StructField('last_updated_ts', StringType(), True),
        StructField('version', StringType(), True),
        StructField('price', FloatType(), True),
        StructField('currency', StringType(), True),
        StructField('free', BooleanType(), True),
        StructField('developer_id', StringType(), True),
        StructField('developer_name', StringType(), True),
        StructField('developer_url', StringType(), True),
        StructField('developer_website', StringType(), True),
        StructField('rating', FloatType(), True),
        StructField('rating_count', IntegerType(), True),
        StructField('current_version_rating', IntegerType(), True),
        StructField('current_version_rating_count', IntegerType(), True)
    ])

    appstore_apps_df = spark.read.csv(
        input_path, header=True, schema=appstore_apps_schema)

    return appstore_apps_df


def process_app_data(spark: SparkSession, appstore_path: str, playstore_path: str, output_path: str):
    '''
    Processing data to create Fact & Dimension tables
    
    Args:
        spark: spark.sql.SparkSession 
            Spark session.
        appstore_path: string
            Data in csv format path (expect to be AppStore data path).
        playstore_path: string
            Data in parquet format path (expect to be PlayStore data path).
        output_path: string 
            Path to write result of the DataFrame
    
    Returns:
        None
    '''
    # read data into dataframe
    appstore_apps_df = read_appstore_data(spark, appstore_path)
    playstore_apps_df = spark.read.parquet(playstore_path)
    
    # filter target data
    appstore_apps_df = appstore_apps_df.filter(col('rating_count') >= 10000)
    
    # cleaning staging data
    appstore_apps_df = appstore_apps_df.fillna(0, ['price']).dropDuplicates()
    playstore_apps_df = playstore_apps_df.fillna(0, ['price', 'score']).dropDuplicates()

    # prevent double slash key when load data to S3
    if output_path[-1] == '/':
        output_path = output_path[:-1]

    # construct developer dimension table
    cols = ['developer_id',
            'developer_name AS name',
            'developer_url AS url',
            'developer_website AS website',
            'CASE WHEN developer IS NOT NULL THEN TRUE ELSE FALSE END AS multi_platform']

    developers_df = appstore_apps_df.join(playstore_apps_df,
                                          col('developer') == col('developer_name'), 'left') \
        .selectExpr(*cols).dropDuplicates(subset=['developer_id'])

    # load data into S3
    developers_df.write.mode('overwrite').parquet(
        f'{output_path}/{DATA_PATH.DEVELOPER}')

    # construct app dimension table
    cols = ['as.app_id AS appstore_app_id',
            'ps.appId AS playstore_app_id',
            'as.app_name AS title',
            'as.genre', 'as.content_rating',
            'as.size',
            'as.price',
            'as.currency',
            'as.free',
            'as.rating AS appstore_rating',
            'ps.score AS playstore_rating',
            'as.rating_count AS rating_count_on_ios',
            'ps.installs']

    as_df = appstore_apps_df.alias('as')
    ps_df = playstore_apps_df.alias('ps')
    apps_df = as_df.join(ps_df, col('as.app_name') == col('ps.title')) \
        .selectExpr(*cols).dropDuplicates()

    # load data into S3
    apps_df.write.mode('overwrite').parquet(
        f'{output_path}/{DATA_PATH.APP}')

    # construct time dimension table
    cols = ['date',
            'EXTRACT(DAY FROM date) AS day',
            'EXTRACT(MONTH FROM date) AS month',
            'EXTRACT(YEAR FROM date) AS year',
            'EXTRACT(WEEK FROM date) AS week',
            'WEEKDAY(date) AS weekday']

    cvt_datetime_to_date_udf = udf(lambda dt: dt.split('T')[0])
    time_df = appstore_apps_df.filter(col('released_ts').isNotNull()) \
        .withColumn('date', cvt_datetime_to_date_udf(col('released_ts')).cast(DateType())) \
        .selectExpr(*cols).filter(length(col('year')) == 4).dropDuplicates()

    # load data into S3
    time_df.write.mode('overwrite').parquet(
        f'{output_path}/{DATA_PATH.TIME}')

    # construct great_app fact table
    cols = ['great_app_id',
            'as.app_id AS appstore_app_id',
            'developer_id',
            'date AS released_date',
            'ROUND((as.rating + ps.score) / 2, 2) AS rating',
            'CASE WHEN as.rating > ps.score THEN TRUE ELSE FALSE END AS better_on_ios',
            'EXTRACT(YEAR FROM date) AS year',
            'EXTRACT(MONTH FROM date) AS month']

    as_df = appstore_apps_df.alias('as')
    ps_df = playstore_apps_df.alias('ps')
    cvt_datetime_to_date_udf = udf(lambda dt: dt.split('T')[0])
    great_apps_df = as_df.filter(col('released_ts').isNotNull() &
                                 col('as.app_id').isNotNull() &
                                 col('developer_id').isNotNull()) \
        .join(ps_df, col('as.app_name') == col('ps.title')) \
        .withColumn('date', cvt_datetime_to_date_udf(col('released_ts')).cast(DateType())) \
        .withColumn('great_app_id', monotonically_increasing_id()) \
        .selectExpr(*cols)

    # load fact table data into S3
    great_apps_df.write.mode('overwrite').parquet(
        f'{output_path}/{DATA_PATH.GREAT_APP}')


def main():
    spark = create_spark_session()

    input_bucket = 'doezaza-s3-endpoint'
    output_bucket = 'doe-dest-output'
    input_path = f's3a://{input_bucket}'
    output_path = f's3a://{output_bucket}'
    

    appstore_path = f'{input_path}/{DATA_PATH.APPSTORE_APP}'
    playstore_path = f'{input_path}/{DATA_PATH.PLAYSTORE_APP}'

    process_app_data(spark, appstore_path, playstore_path, output_path)


if __name__ == '__main__':
    main()
