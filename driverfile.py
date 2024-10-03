import os
import sys
from logging.config import fileConfig

import get_env_variables as evar
from spark_session import get_spark_object
from validate_spark import get_current_date
from extractfiles import extractfiles, display_df
import logging
import logging.config

logging.config.fileConfig('properties/configuration/logging.config')


def main():
    global olap_file, oltp_file, file_format, inferSchema, header
    try:
        logging.info('Initializing Spark context')
        spark = get_spark_object(evar.environment, evar.applicationName)

        logging.info(' Validating Spark context')
        get_current_date(spark)

        olap_file_dir = []
        oltp_file_dir = []

        for oltp_file in os.listdir(evar.src_oltp):
            oltp_file_dir.append(evar.src_oltp + '\\' + oltp_file)

        for olap_file in os.listdir(evar.src_olap):
            olap_file_dir.append(evar.src_olap + '\\' + olap_file)

            if olap_file.endswith('.parquet'):
                file_format = 'parquet'
                header = 'NA'
                inferSchema = 'NA'
            elif olap_file.endswith('.csv'):
                file_format = 'csv'
                header = evar.header
                inferSchema = evar.inferSchema
            elif olap_file.endswith('.json'):
                file_format = 'json'
                header = None
                inferSchema = None

        print(olap_file_dir)

        # df_users = extractfiles(spark=spark, file_dir=olap_file_dir[3], file_format=file_format, header=header,
        #                         inferSchema=inferSchema)
        df_gamesmetadata = extractfiles(spark=spark, file_dir=olap_file_dir[1], file_format=file_format, header=header,
                                        inferSchema=inferSchema)
        # df_games = extractfiles(spark=spark, file_dir=olap_file_dir[0], file_format=file_format, header=header,
        #                         inferSchema=inferSchema)
        # df_recommendations = extractfiles(spark=spark, file_dir=olap_file_dir[2], file_format=file_format,
        #                                   header=header,
        #                                   inferSchema=inferSchema)

        logging.info("Displaying the data frame")
        # display_df(df_users)
        df_gamesmetadata.show()
        # display_df(df_games)
        # display_df(df_recommendations)

    except Exception as e:
        logging.error("An error occurred when calling main() please check the trace ==", str(e))
        sys.exit(1)


if __name__ == '__main__':
    main()
    logging.info("Application Created")
