"""Module which included functions needed to run ETL pipeline"""
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
import logging

logger = logging.getLogger(__name__)

def read_config(  
        config_filename: str,
        path_to_dir: str = None) -> dict:
    
    """
    Read a YAML configuration file.

    Args:
        config_filename (str): The name of the configuration file.
        path_to_dir (str, optional): The path to the directory containing the configuration file.
            Defaults to None.

    Returns:
        dict: The configuration data as a dictionary.
    """
    
    path_to_configfile = os.path.join(
        path_to_dir, 
        config_filename) \
            if path_to_dir is not None else config_filename
    
    with open(path_to_configfile) as yaml_file:
        config = yaml.load(yaml_file, Loader=yaml.SafeLoader)

    return config
    
def get_destination_download_endpoint(
    base_site_url: str,
    results_endpoint: str) -> str:

    response = requests.get(f"{base_site_url}{results_endpoint}")
    soup = BeautifulSoup(response.content, 'html.parser')
    link_elements = soup.find_all('a')

    destination_download_enpoint = [
        link.get('href') for link in link_elements 
        if str(link.get('href')).endswith('Z.tsv.zip')][0]

    return f"{base_site_url}{destination_download_enpoint}"

def compare_version(
    used_version_path: str,
    file_to_download_currently: str) -> bool:

    web_version = file_to_download_currently.split(".")[0]

    with open(used_version_path, 'r', encoding='utf-8') as used_version_json:
        used_version_dict = json.load(used_version_json)

    if not used_version_dict:

        with open(used_version_path, 'w', encoding='utf-8') as used_version_json:
            json.dump({'used_version': web_version}, used_version_json)

        return True

    used_version = list(used_version_dict.values())[0].split(".")[0]
    
    if used_version != web_version:

        with open(used_version_path, 'w', encoding='utf-8') as used_version_json:
            json.dump({'used_version': web_version}, used_version_json)

        return True
    
    return False

def valid_table(
    filename: str,
    needed_tables: list) -> bool:

    for table_name in needed_tables:

        if filename.endswith(table_name):
            return True
        
    return False

def get_number_of_rows(
    path_to_tsv_file: str) -> int:

    with open(path_to_tsv_file, "r", encoding="utf-8") as f:
        num_lines = sum(1 for _ in f)

    return num_lines

def generate_id(input_string):

    id_string = re.sub(r' ', '_', input_string)
    id_string = re.sub(r'[^\w\s-]', '', id_string)

    return id_string

def single_table_pipeline(
        table_name: str,
        data_dir: str,
        extracted_dir: str,
        needed_columns: dict,
        last_number_of_rows: dict,
        sep: str = '\t',
        table_prefix: str = 'WCA_export_',
        file_ext: str = '.tsv') -> pd.DataFrame:
    
    if last_number_of_rows:
    
        rows_cardinalities = last_number_of_rows.get(
            f'{table_prefix}{table_name}{file_ext}', 0)
        
    else:

        rows_cardinalities = 0
   
    path_to_tsv_file = os.path.join(
        data_dir, 
        extracted_dir,
        f'{table_prefix}{table_name}{file_ext}')
    
    new_number_of_rows = get_number_of_rows(
        path_to_tsv_file=path_to_tsv_file
    )

    if rows_cardinalities == new_number_of_rows:
        return None
    
    difference_between_versions = new_number_of_rows - rows_cardinalities

    if difference_between_versions < 0:
        return None
    
    df = pd.read_csv(
            path_to_tsv_file, 
            skiprows=range(1, rows_cardinalities),
            sep=sep,
            usecols=needed_columns.get(table_name))

    return df, new_number_of_rows

def competitions_data_preparation(
    df: pd.DataFrame,
    conn: sqlite3.Connection,
    table_name: str = 'Competitions',
    mapping_columns: dict = {
        'id': 'Id',
        'name': 'Name'
    }) -> pd.DataFrame:

    result = df.copy() \
        .rename(columns=mapping_columns) \
        .dropna() \
        .astype(str)
    
    result = result.drop(columns=[
        col for col in result.columns if col not in list(mapping_columns.values())])
    
    try:
        result.to_sql(table_name, con=conn, if_exists='append', index=False)
    except Exception as e:
        logger.warning(f"Got error when trying to load data into {table_name}:{e}")

    return True

def puzzle_data_preparation(
    df: pd.DataFrame,
    conn: sqlite3.Connection,
    table_name: str = 'Puzzle',
    mapping_columns: dict = {
        'id': 'Id',
        'name': 'Name'}) -> pd.DataFrame:

    result = df.copy() \
        .rename(columns=mapping_columns) \
        .dropna() \
        .astype(str)
    
    result = result.drop(columns=[col for col in result.columns if col not in list(mapping_columns.values())])
    
    try:
        result.to_sql(table_name, con=conn, if_exists='append', index=False)
    except Exception as e:
        logger.warning(f"Got error when trying to load data into {table_name}:{e}")

    return True

def time_data_preparation(
    df: pd.DataFrame,
    conn: sqlite3.Connection,
    table_name: str = 'Time',
    mapping_columns: dict = {
        'day': 'Date_day',
        'month': 'Date_month',
        'year': 'Date_year'}) -> pd.DataFrame:
    
    columns_to_drop = [col for col in df.columns if col not in list(mapping_columns.keys())]

    result = \
        df.copy() \
        .drop(columns=columns_to_drop) \
        .rename(columns=mapping_columns) \
        .dropna() \
        .drop_duplicates() \
        .reset_index() \
        .astype(np.int32)
    
    result['Id'] = result['Date_day'].astype(str) + '_' + result['Date_month'].astype(str) + '_' + result['Date_year'].astype(str)
    result = result.reindex(columns=['Id'] + list(mapping_columns.values()))

    try:
        result.to_sql(table_name, con=conn, if_exists='append', index=False)
    except Exception as e:
        logger.warning(f"Got error when trying to load data into {table_name}:{e}")

    return True

def localization_data_preparation(
    competitions_df: pd.DataFrame,
    countries_df: pd.DataFrame,
    continents_df: pd.DataFrame,
    conn: sqlite3.Connection,
    table_name: str = 'Localization',
    mapping_columns: dict = {'cityName': 'City'},
    required_columns: list = ['City', 'Country', 'Continent']) -> pd.DataFrame:

    competitions_df = competitions_df.copy().rename(columns={'id': 'competitions_id'})
    countries_df = countries_df.copy().rename(columns={'id': 'country_id', 'name': 'Country'})
    continents_df = continents_df.copy().rename(columns={'id': 'continents_id', 'name': 'Continent'})

    result_df = (competitions_df 
        .merge(
            countries_df,
            how='left',
            left_on='countryId',
            right_on='country_id') 
        .merge(
            continents_df,
            how='left',
            left_on='continentId',
            right_on='continents_id')
        .rename(columns=mapping_columns)
        )
    
    result_df = result_df \
        .drop(columns=[col for col in result_df.columns if col not in required_columns]) \
        .dropna() \
        .drop_duplicates() \
        .reset_index()
    
    result_df['Id'] = result_df.apply(
        lambda row: generate_id(f"{row['City'].lower()}_{row['Country'].lower()}"), axis=1)
    
    result_df = result_df \
        .reindex(columns=['Id'] + required_columns)
    
    result_df.drop_duplicates(subset='Id', inplace=True)

    try:
        result_df.to_sql(table_name, con=conn, if_exists='append', index=False)
    except Exception as e:
        logger.warning(f"Got error when trying to load data into {table_name}:{e}")
  
    return True

def nationality_data_preparation(
    countries_df: pd.DataFrame,
    continents_df: pd.DataFrame,
    conn: sqlite3.Connection,
    nationalities_dict: dict,
    table_name: str = 'Nationality',
    required_columns: list = ['Name', 'Continent']) -> pd.DataFrame:

    countries_df_copy = countries_df.copy().rename(columns={'id': 'country_id', 'name': 'Name'})
    continents_df_copy = continents_df.copy().rename(columns={'id': 'continents_id', 'name': 'Continent'})

    result_df = (countries_df_copy 
        .merge(
            continents_df_copy,
            how='left',
            left_on='continentId',
            right_on='continents_id')
        )
    
    result_df = (result_df
        .drop(columns=[
            col for col in result_df.columns if col not in required_columns])
        .dropna()   
        )
    
    result_df['Continent'] = np.where(
        result_df['Continent'] != 'Multiple Continents', 
        result_df['Continent'] + 'n', 
        'Multiple')
    
    result_df['Id'] = result_df['Name'].copy()
    result_df['Name'] = result_df['Id'].apply(lambda x: nationalities_dict.get(x))
    
    result_df = result_df.reindex(columns=['Id'] + required_columns)

    try:
        result_df.to_sql(table_name, con=conn, if_exists='append', index=False)
    except Exception as e:
        logger.warning(f"Got error when trying to load data into {table_name}:{e}")
    
    return True

def attendance_data_preparation(
    results_df: pd.DataFrame,
    competitions_df: pd.DataFrame,
    conn: sqlite3.Connection,
    table_name: str = 'Attendance',
    mapping_columns: dict = {
        'eventId': 'Puzzle_id',
        'competitionId': 'Competition_id',
        'personCountryId': 'Nationality_id',
        'countryId': 'Country_id'},
    required_columns: list = [
        'Competition_id',
        'Localization_id',
        'Puzzle_id', 
        'Nationality_id',
        'Time_id']) -> pd.DataFrame:

    attendance_df = (results_df.copy()
        .merge(
            competitions_df.copy(),
            how='left',
            left_on='competitionId',
            right_on='id')
        .rename(columns=mapping_columns))
    
    attendance_df['Time_id'] = \
        attendance_df['day'].astype(str) \
        + '_' \
        + attendance_df['month'].astype(str) \
        + '_' \
        + attendance_df['year'].astype(str)
    
    attendance_df['Localization_id'] = attendance_df.apply(
        lambda row: generate_id(f"{row['cityName'].lower()}_{row['Country_id'].lower()}"), axis=1)
    
    attendance_df = (attendance_df
        .drop(columns=[
            col for col in attendance_df.columns if col not in required_columns])
        .dropna()   
        .reset_index(drop=True))
    
    attendance_df['Number_of_participants'] = \
        attendance_df.groupby(list(attendance_df.columns)).transform('size')
    
    attendance_df = (attendance_df
        .reindex(columns=['Number_of_participants'] + required_columns)
        .drop_duplicates()
        .reset_index(drop=True))
    
    try:
        attendance_df.to_sql(table_name, con=conn, if_exists='append', index=False)
    except Exception as e:
        logger.warning(f"Got error when trying to load data into {table_name}:{e}")

    return True