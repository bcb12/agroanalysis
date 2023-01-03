# Imports
import logging
import re
import pandas as pd
import os
import psycopg2
import psycopg2.extras as extras
import requests
from sqlalchemy import create_engine
from config import config
from time import perf_counter
from multipledispatch import dispatch

# Variables
DATA_PATH = os.getcwd()+"/data/"
PRODUCTS_DICT = DATA_PATH + 'product_rename_clean.csv'
FINAL_TABLE_NAME = "dataset_final"

'''
Timer decorator
'''
def timer(fn):
    def inner(*args, **kwargs):
        start_time = perf_counter()
        to_execute = fn(*args, **kwargs)
        end_time = perf_counter()
        execution_time = end_time - start_time
        print('{0} took {1:.2f}s to execute'.format(fn.__name__, execution_time))
        to_execute['elapsed_time'] = round(execution_time, 2)
        return to_execute
    
    return inner

'''
Build response
'''
def build_response(dataset_name):
    response = {
        "files": dataset_name,
        "success": False,
        "elapsed_time": False
    }

    return response

'''
Generate dataset_final
'''
@timer
def generate_dataset_final():
    response = build_response('dataset_final')

    # Datasets imports
    df_1 = get_ds('dataset_1_clean')
    df_3a = get_ds('dataset_3a_clean')
    df_3b = get_ds('dataset_3b_clean')
    df_4 = get_ds('dataset_4_clean')
    df_5 = get_ds('dataset_5_clean')

    # Datasets merge
    df = merge_df([df_1, df_3a, df_3b, df_4, df_5])

    # Export merged dataset to DB
    manage_insert_to_db(FINAL_TABLE_NAME, df)

    response['success'] = True
    return response

'''
Get dataset from DB, clean it and export it to DB
'''
@timer
def clean_dataset(dataset_name):
    output_dataset_name = dataset_name.lower()+"_clean"
    response = build_response(dataset_name)
    df = None
    match dataset_name:
        case "dataset_1":
            df = df_1_cleaning(get_ds(output_dataset_name))
        case "dataset_3a":
            df = df_3a_cleaning(get_ds(output_dataset_name))
        case "dataset_3b":
            df = df_3b_cleaning(get_ds(output_dataset_name))
        case "dataset_4":
            df = df_4_cleaning(get_ds(output_dataset_name))
        case "dataset_5":
            df = df_5_cleaning(get_ds(output_dataset_name))

    print(df)

    # TODO: check if the operation was successful
    manage_insert_to_db(output_dataset_name, df)

    response['success'] = True
    return response

'''
Insert dataset to DB
'''
def manage_insert_to_db(dataset_name, df):
    create_table_if_needed(dataset_name, df)
    truncate_table(dataset_name)
    insert_values(dataset_name, columns_to_str(df.columns.tolist()), df)

'''
Check if table exists in database
'''
def check_table(table_name):
    exists = False
    params = config()
    conn = psycopg2.connect(**params)
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s;", (table_name,))
    if(cur.rowcount > 0):
        exists = True
    cur.close()
    conn.close()
    return exists

'''
Return dataset from DB
'''
def get_ds(table):
    df = None

    if(check_table(table)):
        df = pd.DataFrame(import_from_db(table))
    elif(table == "dataset_final"):
        df = generate_dataset_final()
    else:
        regex_raw = r"dataset_(1|3a|3b|4|5)_raw"
        regex_clean = r"dataset_(1|3a|3b|4|5)_clean"

        if(re.match(regex_clean, table)):
            raw_ds_name = table.replace("clean", "raw")
            raw_ds = get_ds(raw_ds_name) # Here goes raw_ds_name
            print(raw_ds)
            match table:
                case "dataset_1_clean":
                    df = df_1_cleaning(raw_ds)
                case "dataset_3a_clean":
                    df = df_3a_cleaning(raw_ds)
                case "dataset_3b_clean":
                    df = df_3b_cleaning(raw_ds)
                case "dataset_4_clean":
                    df = df_4_cleaning(raw_ds)
                case "dataset_5_clean":
                    df = df_5_cleaning(raw_ds)

        elif(re.match(regex_raw, table)):
            simple_table_name = table.replace('_raw', '')
            url = "http://172.18.0.10:85/extraction/todb/"+simple_table_name
            print(url)
            response = requests.get(url, timeout=180)

            print(response)

            if(response.status_code == 200):
                df = pd.DataFrame(import_from_db(table))

    return df

# Import data from postgresql database to a pandas dataset
def import_from_db(table):
    params = config()
    db = params

    engine_string = "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}".format(
        user = db['user'],
        password = db['password'],
        host = db['host'],
        port = db['port'],
        database = db['database'],
    )

    engine = create_engine(engine_string)
    data = pd.read_sql_table(table,engine)
    return data


# Dataset 1 cleaning (most of it was done at the extraction to transform step)
def df_1_cleaning(df):
    # Rename products name to match other datasets standard
    df = normalize_products(df, 'Dataset 1')
    return df


# Dataset 3a cleaning
def df_3a_cleaning(df):
    # Rename products name to match other datasets standard 
    df = normalize_products(df, 'Dataset 3a')
    
    # Drop unwanted columns
    df.drop(['variedad','origen', 'unidad', 'familia','price_min','price_max'], axis=1, inplace=True) # inplace=True
    
    # Group columns product, year and month
    df = df.groupby(['product', 'year', 'month']).sum().reset_index()
    return df


# Dataset 3b cleaning
def df_3b_cleaning(df):
    # Rename products name to match other datasets standard 
    df = normalize_products(df, 'Dataset 3b')
    
    # Drop unwanted columns
    df.drop(['origen', 'unidad', 'familia'], axis=1, inplace=True) # inplace=True
    
    # Group columns product, year and month
    df = df.groupby(['product', 'year', 'month']).sum().reset_index()
    return df


# Dataset 4 cleaning
def df_4_cleaning(df):
    df = normalize_products(df, 'Dataset 4')

    # Fix Belgium name
    df['reporter'].replace(r'Belgium.*', 'Belgium', regex = True, inplace = True)

    # Fix France (France is unfixable)
    df['reporter'].replace(r'France.*', 'France', regex = True, inplace = True)

    # Fix Germany
    df['reporter'].replace(r'Germany.*', 'Germany', regex = True, inplace = True)

    # Fix Ireland
    df['reporter'].replace(r'Ireland.*', 'Ireland', regex = True, inplace = True)

    # Fix Italy
    df['reporter'].replace(r'Italy.*', 'Italy', regex = True, inplace = True)

    # Fix Spain
    df['reporter'].replace(r'Spain.*', 'Spain', regex = True, inplace = True)

    # Drop rows where reporter is "European Union"
    df = df[df["reporter"].str.contains("European Union.*") == False]

    # Format period column to be able to strip it to two separate cols: year and month
    format = '[A-Z][a-z]{2}\.-[A-Z][a-z]{2}\. 20[0-9]{2}'
    df = df[df['time_period'].str.contains(format,regex=True) == False]
    df['time_period'] = pd.to_datetime(df['time_period'], format = '%Y-%m')

    # Create columns year and month, and drop period
    df.insert(0, 'year', df['time_period'].dt.year)
    df.insert(1, 'month', df['time_period'].dt.month)
    del df['time_period']

    # Replace null value placeholder ":" for 0
    df['obs_value'].replace(r':', 0, regex = True, inplace = True)

    # Delete entries which indicator is VALUE_IN_EUROS and which flow is EXPORT
    df = df[df.indicators.isin(['VALUE_IN_EUROS']) == False]
    df = df[df.flow.isin(['EXPORT']) == False]

    # Drop unwanted columns
    df.drop(['partner','flow', 'indicators'], axis=1, inplace=True)

    # Rename reporter column to origin_country
    df.rename(columns = {'reporter': 'origin_country'}, inplace=True)

    return df


# Dataset 5 cleaning
def df_5_cleaning(df):
    # Drop non-relevant month entries
    df.drop(df[((df.day < 14) & (df.month == 1))].index, inplace=True,axis = 0)

    # Convert dates to datetime type
    df['daterep'] = pd.to_datetime(df['daterep'])

    # Remove unwanted columns
    df = df.drop(columns = ["day","cumulative_number_for_14_days_of_covid19_cases_per_100000","daterep","popdata2019","countryterritorycode","geoid","continentexp"])

    # Discard rows where country/territory value is Cases_on_an_international_conveyance_Japan
    df = df[df.countriesandterritories != "Cases_on_an_international_conveyance_Japan"]

    # Rename countries column to origin_country
    df.rename(columns = {'countriesandterritories': 'origin_country'}, inplace=True)

    # Group by month, year and origin_country
    df = df.groupby(["month","year","origin_country"]).agg({'cases':'sum','deaths':'sum'})

    # Create a column with sum of cases and deaths columns
    df['cases_and_deaths'] = df["cases"] + df["deaths"]

    return df


# Normalize products names
def normalize_products(df, dataset_name):
    print(df)
    df['product'] = df['product'].str.rstrip()
    source_products = pd.DataFrame(pd.read_csv(PRODUCTS_DICT))
    useful_products = source_products[dataset_name]
    useful_products.dropna(inplace=True)

    df = df[df['product'].isin(useful_products)]
    source_dict = source_products[[dataset_name, dataset_name + ' N']]
    dictionary = dict(source_dict.values)
    dictionary.popitem()
    df = df.replace({"product":dictionary})
    df.reset_index(inplace = True)
    df.drop(columns=df.columns[0], axis=1, inplace=True)
    
    df.dropna(axis=0, subset=['product'], inplace=True)

    return df


# Merge dataframes given as a list of dataframes
def merge_df(dfs):
    df_count = len(dfs)

    match df_count:
        case 0:
            print("No df given, you need to provide 2 at least.")
        case 1:
            print("Only one df found, you need to provide 2 at least.")
        case 2:
            df = pd.merge(dfs[0], dfs[1], on=['product', 'year', 'month'])
            print("No df given, you need to provide 2 at least.")
        case 3:
            df = pd.merge(dfs[0], dfs[1], on=['product', 'year', 'month'])
            df = pd.merge(df, dfs[2], on=['product', 'year', 'month'])
            print("No df given, you need to provide 2 at least.")
        case 4:
            df = pd.merge(dfs[0], dfs[1], on=['product', 'year', 'month'])
            df = pd.merge(df, dfs[2], on=['product', 'year', 'month'])
            df = pd.merge(df, dfs[3], on=['product', 'year', 'month'])
            print("No df given, you need to provide 2 at least.")
        case 5:
            df = pd.merge(dfs[0], dfs[1], on=['product', 'year', 'month'])
            df = pd.merge(df, dfs[2], on=['product', 'year', 'month'])
            df = pd.merge(df, dfs[3], on=['product', 'year', 'month'])
            df = pd.merge(df, dfs[4], on=['year','month','origin_country'])

    return df

'''
Truncate specified table
'''
def truncate_table(table):
    # Connection to DB
    params = config()
    conn = psycopg2.connect(**params)
    remove_query = "truncate " + table + ";"
    cursor = conn.cursor()

    # Make query
    try:
        cursor.execute(remove_query)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    logging.info("The database has been truncated")
    cursor.close()
    return 0

'''
Create a table with the specified columns
'''
def create_table_if_needed(table_name, df):
    # Create a list that will contain the columns names and their types
    columns = []
    for col in df.columns:
        if df[col].dtype == 'object':
            columns.append([col, 'VARCHAR'])
        elif df[col].dtype == 'int64':
            columns.append([col, 'BIGINT'])
        elif df[col].dtype == 'float64':
            columns.append([col, 'FLOAT'])
        elif df[col].dtype == 'datetime64[ns]':
            columns.append([col, 'TIMESTAMP'])
        else:
            columns.append([col, 'VARCHAR'])

    # Connection to DB
    params = config()
    conn = psycopg2.connect(**params)

    # Create table query
    query = "CREATE TABLE IF NOT EXISTS %s (" % table_name
    for column in columns:
        query += column[0] + " " + column[1] + ", "
    query = query[:-2]
    query += ");"

    # Make query
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    logging.info("Table created successfully")
    cursor.close()
    return 0

'''
Insert dataframe into a given table with the specified columns
'''
def insert_values(table, cols, data):
    # Connection to DB
    params = config()
    conn = psycopg2.connect(**params)
    tuples = [tuple(x) for x in data.to_numpy()]
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()

    # Make query
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1

    logging.info("The dataframe was inserted")
    cursor.close()
    return 0

def columns_to_str(df_columns):
    str_columns = ""
    for column in df_columns:
        str_columns += column + ", "
    str_columns = str_columns[:-2]
    return str_columns