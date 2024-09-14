import logging.config
from pyspark.sql import SparkSession


logging.config.fileConfig('properties/configuration/logging.config')

logger = logging.getLogger('Spark_session')


def get_spark_object(env_variable, appName):
    try:
        logger.info('Spark_Session method started')

        if env_variable == 'DEV':
            master = 'local'

        else:
            master = 'Yarn'

        logger.info('master is {}'.format(master))

        spark = SparkSession.builder.master(master).appName(appName).getOrCreate()

    except Exception as exp:
        logger.error("An error occured in spark_session method ===", str(exp))
        raise

    else:
        logger.info('Spark session created')

    return spark
