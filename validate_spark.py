import logging.config

logging.config.fileConfig('properties/configuration/logging.config')

loggers = logging.getLogger('Validate')


def get_current_date(spark):
    try:
        loggers.warning('Started the current_date method ...')
        output = spark.sql("""Select current_date""")
        loggers.warning("validating spark with current date" + str(output.collect()))

    except Exception as e:
        loggers.error("An error occured in validate_spark Please check the trace...", str(e))
        raise

    else:
        loggers.warning('Validation done')
