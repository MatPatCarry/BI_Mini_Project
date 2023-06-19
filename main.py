import requests
import zipfile
import sqlite3
import pandas as pd
import os
from bs4 import BeautifulSoup
import yaml
import json
from multiprocessing import Pool
from contextlib import closing
import re
import numpy as np
import sys
import logging
from functools import partial

from utils.module_functions import \
    read_config, \
    get_destination_download_endpoint, \
    compare_version, \
    valid_table, \
    single_table_pipeline, \
    competitions_data_preparation, \
    puzzle_data_preparation, \
    time_data_preparation, \
    localization_data_preparation, \
    nationality_data_preparation, \
    attendance_data_preparation
    
if __name__ == '__main__':

    CONFIGURATION = read_config(config_filename='CONFIG.yaml')

    LOGGER = CONFIGURATION.get('LOGGER')
    LOGGING_FORMAT = LOGGER.get('logging_format')
    LOGGER_LEVEL = LOGGER.get('logger_level')

    logging.basicConfig(
        level=LOGGER_LEVEL,
        format=LOGGING_FORMAT)

    logger = logging.getLogger(__name__)

    BASE_SITE_URL = CONFIGURATION.get('BASE_SITE_URL')
    RESULTS_ENDPOINT = CONFIGURATION.get('RESULTS_ENDPOINT')
    DATA_DIR = CONFIGURATION.get('DATA_DIR')
    EXTRACTED_DATA_DIR = CONFIGURATION.get('EXTRACTED_DATA_DIR')
    NEEDED_TABLES = CONFIGURATION.get('NEEDED_TABLES')
    EXT_NAME = CONFIGURATION.get('EXT_NAME')
    USED_VERSION_FILE = CONFIGURATION.get('USED_VERSION_FILE')
    DB_NAME = CONFIGURATION.get('DB_NAME')
    CREATES_SQL_FILE = CONFIGURATION.get('CREATES_SQL_FILE')
    NATIONALITIES_FILE = CONFIGURATION.get('NATIONALITIES_FILE')
    NUMBER_OF_ROWS_FILE = CONFIGURATION.get('NUMBER_OF_ROWS_FILE')
    WCA_PREFIX = CONFIGURATION.get('WCA_PREFIX')

    destination_download_endpoint = get_destination_download_endpoint(
        base_site_url=BASE_SITE_URL,
        results_endpoint=RESULTS_ENDPOINT
    )

    logger.info(f'Destination endpoint: {destination_download_endpoint}')

    file_name = os.path.basename(destination_download_endpoint)

    if not compare_version(os.path.join(DATA_DIR, USED_VERSION_FILE), file_name):
        logger.info('Everything up to date')
        # sys.exit(0)

    response = requests.get(destination_download_endpoint)

    if response.status_code == requests.codes.ok:

        logger.info('Successfully downloaded data')

        save_path = os.path.join(
            DATA_DIR, 
            file_name)

        with open(save_path, 'wb') as file_:
            file_.write(response.content)

        logger.info('Successfully saved data locally')

    else:
        logger.error('Can not download new data!')
        sys.exit(0)

    with zipfile.ZipFile(save_path, 'r') as zip_ref:

        zip_ref.extractall(os.path.join(
            DATA_DIR, 
            EXTRACTED_DATA_DIR)
        )

        logger.info('Successfully extraced ZIP')
    
    for file_ in os.listdir(os.path.join(DATA_DIR, EXTRACTED_DATA_DIR)):

        filename, extension = os.path.splitext(file_)
        
        if extension == EXT_NAME and not valid_table(filename, needed_tables=NEEDED_TABLES.keys()):

            try:
                os.remove(os.path.join(DATA_DIR, EXTRACTED_DATA_DIR, file_))
            except Exception as e:
                logging.error(f'Got error: {e}')
            else:
                continue

    with open(os.path.join(DATA_DIR, NUMBER_OF_ROWS_FILE)) as rows_cardinalities_json:
        rows_cardinalities = json.load(rows_cardinalities_json)

    logger.info('Starting fething data into DataFrames')

    dfs_result = {
        table_name: single_table_pipeline(
            table_name=table_name,
            data_dir=DATA_DIR,
            extracted_dir=EXTRACTED_DATA_DIR,
            needed_columns=NEEDED_TABLES,
            last_number_of_rows=rows_cardinalities)

        for table_name in NEEDED_TABLES.keys()
    }

    logger.info('Successufully fetched data')

    conn = sqlite3.connect(DB_NAME)

    logger.info('Connected to DB')

    with open(os.path.join(DATA_DIR, CREATES_SQL_FILE), 'r', encoding='utf-8') as creates_sql:

        sql_file_content = creates_sql.read()
        sql_statements = sql_file_content.split('\n\n')
        logger.info('Read SQL CREATE statements')

    with closing(conn.cursor()) as cur:

        for create_statement in sql_statements:

            cur.execute(create_statement)
            conn.commit()

        logger.info('Successfully executed CREATE statements')

    competitions_data_preparation(
        df=dfs_result.get('Competitions')[0],
        conn=conn
    )

    logger.info('Loaded data to Competition table')

    puzzle_data_preparation(
        df=dfs_result.get('Events')[0],
        conn=conn
    )

    logger.info('Loaded data to Puzzle table')

    time_data_preparation(
        df=dfs_result.get('Competitions')[0],
        conn=conn
    )

    logger.info('Loaded data to Time table')

    localization_data_preparation(
        competitions_df=dfs_result.get('Competitions')[0],
        countries_df=dfs_result.get('Countries')[0],
        continents_df=dfs_result.get('Continents')[0],
        conn=conn
    )

    logger.info('Loaded data to Localization table')

    with open(os.path.join(DATA_DIR, NATIONALITIES_FILE), 'r', encoding='utf-8') as nationalities_json:
        nationalities_dict = json.load(nationalities_json)

    nationality_data_preparation(
        countries_df=dfs_result.get('Countries')[0],
        continents_df=dfs_result.get('Continents')[0],
        nationalities_dict=nationalities_dict,
        conn=conn
    )

    logger.info('Loaded data to Nationality table')

    attendance_data_preparation(
        results_df=dfs_result.get('Results')[0],
        competitions_df=dfs_result.get('Competitions')[0],
        conn=conn
    )

    logger.info('Loaded data to Attendance table')

    new_rows_cardinalities = {}

    for table_name, (_, n_of_rows) in dfs_result.items():
        new_rows_cardinalities[f"{WCA_PREFIX}{table_name}{EXT_NAME}"] = n_of_rows

    with open(os.path.join(DATA_DIR, NUMBER_OF_ROWS_FILE), 'w', encoding='utf-8') as rows_cardinalities_json:
        json.dump(new_rows_cardinalities, rows_cardinalities_json)
    
    logger.info('Updated file with rows cardinalities')

    logger.info('ETL performed successfully :D')

    conn.close()

    # partial_pipeline = partial(single_table_pipeline,
    #                            data_dir=DATA_DIR,
    #                            extracted_dir=EXTRACTED_DATA_DIR,
    #                            needed_columns=NEEDED_TABLES,
    #                            last_number_of_rows=0)

    # with Pool(4) as pool:
    #     dfs_result = pool.map(partial_pipeline, NEEDED_TABLES.keys())
        
    # for res in dfs_result:
    #     print(res)

        
        