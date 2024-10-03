import logging.config

logging.config.fileConfig('properties/configuration/logging.config')

logger = logging.getLogger('Extractfile')


def extractfiles(spark, file_dir, file_format, header, inferSchema):
    global olap_file_dir, oltp_file_dir
    try:
        logger.warning("File extraction started ....")
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).option('header', header).option('inferSchema', inferSchema).load(
                file_dir)
        elif file_format == 'json':
            df = spark.read.format(file_format).load(file_dir)
            df.show()

    except Exception as e:
        logger.error("An error occured at extractfiles", e)
        raise

    else:
        logger.warning("Dataframe is created successfully is of {}".format(file_format))

    return df


def display_df(df):
    df_show = df.show()

    return df_show
