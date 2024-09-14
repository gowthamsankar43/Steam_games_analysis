import sys

import get_env_variables as evar
from spark_session import get_spark_object
from validate_spark import get_current_date
import logging
import logging.config

logging.config.fileConfig('properties/configuration/logging.config')


def main():
    try:
        logging.info('Initializing Spark context')
        spark = get_spark_object(evar.environment, evar.applicationName)

        logging.info(' Validating Spark context')
        get_current_date(spark)
    except Exception as e:
        logging.error("An error occurred when calling main() please check the trace ==", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info("Application Created")
