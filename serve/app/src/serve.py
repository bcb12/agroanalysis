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
def get_dataset(table):
    df = None
    if table == "final_ds":
        df = pd.DataFrame(import_from_db('dataset_final'))
    elif(check_table(table)):
        df = pd.DataFrame(import_from_db(table))
    else:
        regex_raw = r"dataset_(1|3a|3b|4|5)_raw"
        regex_clean = r"dataset_(1|3a|3b|4|5)_clean"

        if(re.match(regex_clean, table)):
            simple_table_name = table.replace('_clean', '')
            url = "http://172.18.0.11:86/load/clean/"+simple_table_name
            response = requests.get(url, timeout=180)

            if(response.status_code == 200):
                df = pd.DataFrame(import_from_db(table))

        elif(re.match(regex_raw, table)):
            simple_table_name = table.replace('_raw', '')
            url = "http://172.18.0.10:85/extraction/todb/"+simple_table_name
            response = requests.get(url, timeout=180)

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
