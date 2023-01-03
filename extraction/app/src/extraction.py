'''Import declarations'''
import re
import logging
import os
import urllib.request
import gzip
from time import perf_counter
import requests
import pandas as pd
from bs4 import BeautifulSoup
import psycopg2
from psycopg2 import extras
from multipledispatch import dispatch
from config import config


logging.basicConfig(
    format='%(asctime)s-%(filename)s-%(levelname)s: %(message)s',
    level=logging.DEBUG
)

ROOT_PATH = os.getcwd()
DATA_PATH = "data/"
RAW_DATASETS_PATH = DATA_PATH + "raw_data/"

def timer(input_funcion):
    '''
    Timer decorator
    '''
    def inner(*args, **kwargs):
        '''
        Inner timer logic
        '''
        start_time = perf_counter()
        to_execute = input_funcion(*args, **kwargs)
        end_time = perf_counter()
        execution_time = end_time - start_time
        logging.info('%s took %.2fs to execute' % (input_funcion.__name__, execution_time))
        to_execute['elapsed_time'] = round(execution_time, 2)
        return to_execute
    return inner

@dispatch(list, str)
def build_response(file_name, location):
    '''
    Response builder for extraction operations
    '''
    response = {
        "files": file_name,
        "location": location,
        "success": False,
        "elapsed_time": False
    }

    return response

@dispatch(list, str, str)
def build_response(source_files, table, cols):
    '''
    Response builder for database import operations
    '''
    response = {
        "files": source_files,
        "table": table,
        "columns": cols,
        "success": False,
        "elapsed_time": False
    }

    return response

@timer
def data_set_1_extract():
    '''
    Retrieve dataset 1 xlsx from the mapa.gob.es web
    '''
    # Variable declarations
    base_site_url = 'https://www.mapa.gob.es'
    page_content = requests.get(
        'https://www.mapa.gob.es/es/alimentacion/temas/consumo-tendencias/panel-de-consumo-alimentario/series-anuales/',
        timeout=20
    )
    data_set_1_path = "/" + RAW_DATASETS_PATH + 'dataset_1/'
    year_from = 18
    year_to = 20
    regex_ccaa = re.compile(' CCAA ??')

    # Response body
    file_names = []
    response = build_response(file_names, data_set_1_path)
    # Web content scrapping
    html_soup = BeautifulSoup(page_content.content, 'html.parser')
    for element in html_soup.find_all("a", {"title": regex_ccaa}):
        year = int(element.get('title')[-2:])
        # Only datasets between the years specified will be retrieved
        if year_from <= year <= year_to:
            # Make request
            url = base_site_url + element.get('href')
            req_file = requests.get(url, timeout=20)

            # Create file in system
            file_title = element.get('title').replace(" ", "_") + '.xlsx'
            file_names.append(file_title)
            with open(ROOT_PATH + data_set_1_path + file_title, 'wb') as file:
                file.write(req_file.content)

            # Update response and display info
            response["success"] = True
            logging.info("%s downloaded to %s" % (file_title, data_set_1_path))

    return response

# TODO: Retrieve dataset 3a csv from Merca Madrid web
'''
def data_set_3a_extract():
    return "dataset_3a"
'''

# TODO: Retrieve dataset 3b csv from Merca Barna web
'''
def data_set_3b_extract():

'''

@timer
def data_set_4_extract():
    '''
    Retrieve dataset 4 csv from ec.europa.eu web (specific query)
    '''
    # Variable declarations
    dataset = 'https://ec.europa.eu/eurostat/api/comext/dissemination/sdmx/2.1/data/data_set-045409/M.DE+BE+FI+PT+BG+DK+LT+LU+HR+LV+FR+HU+SE+SI+SK+EU27_2020+IE+EE+MT+GR+IT+ES+AT+CY+CZ+PL+RO+NL.ES.070890+07049010+070810+070820+08062010+080410+080430+080440+080450+08062090+08062030+070970+070910+070920+070930+070940+070410+070420+070320+070610+070511+070519+070521+070529+08059000+080929+080522+08052190+08052110+080720+08093090+08081080+08093010+08103010+08103030+08091000+08054000+08042010+08030011+08030019+07031019+07031090+07096010+07096099+08071010+08071090+08083090+07070090+08105000+07070005+08134030+08134050+07099310+08134010+07099050+07099060+07099031+07099039+07099040+08061010+08061090+081060+081070+081010+081310+081320+081330+0702+08051028+08051024+08051022+07019010+07019050+07019090+08055010+08092005+08055090+08102010+08102090+07095950+07095990+08082090+08094005+07095150+07095130+08094090+07095100.1+2.QUANTITY_IN_100KG+VALUE_IN_EUROS/?format=SDMX-CSV&compressed=true&returnData=ALL&startPeriod=2018-01&endPeriod=2020-12&lang=en&label=label_only'
    file_title = 'dataset_4.csv'
    data_set_4_path = "/" + RAW_DATASETS_PATH + 'dataset_4/'

    # Response body
    response = build_response([file_title], data_set_4_path)

    try:
        # Make request
        with urllib.request.urlopen(dataset) as req_file:

            # Unzip retrieved file
            with gzip.GzipFile(fileobj=req_file) as data:
                file_content = data.read()

        # Create file in the system
        with open(ROOT_PATH + data_set_4_path + file_title, 'wb') as file:
            file.write(file_content)

            # Update response and display info
            response["success"] = True
            logging.info("%s downloaded to %s" % (file_title, RAW_DATASETS_PATH))
    except Exception as e:
        response["success"] = False
        logging.error('Error: %s', e)

    return response

@timer
def data_set_5_extract():
    '''
    Retrieve dataset 5 csv from opendata.ecdc.europa.eu web
    '''
    # Variable declarations
    file_title = 'dataset_5.xlsx'
    data_set_5_path = "/" + RAW_DATASETS_PATH + 'dataset_5/'

    # Response body
    response = build_response([file_title], data_set_5_path)

    # Make request
    data = requests.get(
        'https://opendata.ecdc.europa.eu/covid19/casedistribution/csv/data.csv',
    timeout=20
    )

    # Create file in the system
    with open(ROOT_PATH + data_set_5_path + file_title, 'wb') as file:
        file.write(data.content)

        # Update response and display info
        response["success"] = True
        logging.info("%s downloaded to %s" % (file_title, RAW_DATASETS_PATH))

    return response

@timer
def data_set1_to_db():
    '''
    Insert values from the dataset_1 original files to it's postgresql table
    '''
    # Variable declarations
    datasets_1_path = "/" + RAW_DATASETS_PATH + 'dataset_1/'
    table = 'dataset_1_raw'
    cols = 'Product, Year, Month, Per_Capita_Consum, Penetration, Avg_Weight, Volume_kg'
    source_files = sorted(next(os.walk(ROOT_PATH + datasets_1_path))[2])
    # Check if the dataset_1 folder is empty
    if len(source_files) == 0:
        # If it is, download the files
        data_set_1_extract()
        source_files = sorted(next(os.walk(ROOT_PATH + datasets_1_path))[2])
    # Response body
    response = build_response(source_files, table, cols)

    # Remove DB if exists
    truncate_table(table)
    # Iterate through the datasets (years)
    data_set_counter = 0
    for dataset in source_files:
        year = "20" + dataset.split("_")[-1].split(".")[0]
        data_set_counter += 1
        sheet_counter = 0
        # Iterate through the excel sheets (months)
        while sheet_counter<12:
            sheet_counter+=1
            # Convert the source file to a dataframe
            data_set = pd.read_excel(
                ROOT_PATH + datasets_1_path + dataset,
                sheet_name=sheet_counter,
                index_col=None,
                usecols = 'A:G',
                header=2
            )
            data_frame = pd.DataFrame(data_set)

            # Rename the product column with no original name to Product
            data_frame.rename(columns = {'Unnamed: 0': 'Product'}, inplace=True)

            # Add Year column
            data_frame['Year'] = int(year)
            data_frame['Month'] = sheet_counter

            # Remove unwanted columns
            data_frame.drop(
                ['VALOR (Miles Euros)', 'GASTO X CAPITA'],
                axis=1,
                inplace=True
            )

            # Group rows by product, year and month, and sum the values
            data_frame = data_frame.groupby(['Product', 'Year', 'Month']).sum().reset_index()

            # Rename columns
            data_frame.rename(
                columns = {
                    'VOLUMEN (Miles kg ó litros)': 'Volume_kg',
                    'PRECIO MEDIO kg ó litros': 'Avg_Weight',
                    'PENETRACION (%)': 'Penetration',
                    'CONSUMO X CAPITA': 'Per_Capita_Consum'
                },
                inplace=True
            )

            # Create DB if not exists and insert values into db
            if(data_set_counter == 1 and sheet_counter == 1):
                logging.info("Creating table %s", table)
                create_table_if_needed(table, data_frame)
            insert_values(table, columns_to_str(data_frame.columns.tolist()), data_frame)

    # Update response and return it
    response["success"] = True
    return response

@timer
def data_set3a_to_db():
    '''
    Insert values from the dataset_3a original file to it's postgresql table
    '''
    # Variable declarations
    source_file = 'Dataset3a_CONVERTED.txt'
    table = 'dataset_3a_raw'
    cols = 'product, variety, origin, unit, family, year, month, price_mean, price_min, price_max, volume'
    dataset_3a_path = RAW_DATASETS_PATH + 'dataset_3a/' + source_file

    # Check if the dataset_3a file exists
    data_frame = get_file(dataset_3a_path)
    # Response body
    response = build_response([source_file], table, cols)

    # Insert data into DB
    manage_insert_to_db(table, data_frame)

    # Update response and return it
    response["success"] = True
    return response

@timer
def data_set3b_to_db():
    '''
    Insert values from the dataset_3b original file to it's postgresql table
    '''
    # Variable declarations
    source_file = 'Dataset3b_CONVERTED.txt'
    dataset_3b_path = RAW_DATASETS_PATH + 'dataset_3b/' + source_file
    table = 'dataset_3b_raw'
    cols = 'product, origin, unit, family, year, month, price_mean, volume'

    # Check if the dataset_3a file exists
    data_frame = get_file(dataset_3b_path)

    # Response body
    response = build_response([source_file], table, cols)
    # Insert data into DB
    manage_insert_to_db(table, data_frame)

    # Update response and return it
    response["success"] = True
    return response

@timer
def data_set4_to_db():
    '''
    Insert values from the dataset_4 original file to it's postgresql table
    '''
    # Variable declarations
    source_file = 'Dataset_4.csv'
    dataset_4_path = RAW_DATASETS_PATH + 'dataset_4/' + source_file
    table = 'dataset_4_raw'
    cols = 'period, reporter, partner, product, flow, indicators, value'

    # Check if the dataset_3a file exists
    data_frame = get_file(dataset_4_path)

    # Response body
    response = build_response([source_file], table, cols)
    # Drop unwanted columns
    data_frame.drop(
        [
            'DATAFLOW',
            'LAST UPDATE',
            'freq',
            'OBS_FLAG'
        ], axis=1, inplace=True
    )

    # Change column order
    data_frame = data_frame[
        [
            'TIME_PERIOD',
            'reporter',
            'partner',
            'product',
            'flow',
            'indicators',
            'OBS_VALUE'
        ]
    ]

    # Insert data into DB
    manage_insert_to_db(table, data_frame)

    # Update response and return it
    response["success"] = True
    return response

@timer
def data_set5_to_db():
    '''
    Insert values from the dataset_5 original file to it's postgresql table
    '''
    # Variable declarations
    source_file = 'Dataset_5.xlsx'
    dataset_5_path = RAW_DATASETS_PATH + 'dataset_5/' + source_file
    table = 'dataset_5_raw'
    cols = 'dateRep, day, month, year, cases, deaths, countries, geoId, countryTerritoryCode, popData2019, continentExp, cumulativeNumber14DaysCov19'

    # Check if the dataset_3a file exists and retrieve it
    data_frame = get_file(dataset_5_path)

    # Response body
    response = build_response([source_file], table, cols)

    # Insert data into DB
    manage_insert_to_db(table, data_frame)

    # Update response and return it
    response["success"] = True
    return response

def get_file(file_path):
    print(file_path)
    '''
    Retrieve data from the source file if it exists
    '''
    # Try to find the file
    try:
        if os.path.exists(file_path):
            # Convert the source file to a dataframe
            data_frame = pd.DataFrame(pd.read_csv(file_path))
            data_frame = data_frame.rename(columns=lambda x: x.replace('-', ''))
            return data_frame
        file_name = os.path.basename(file_path)
        if file_name not in ("dataset_3a_CONVERTED.txt", "dataset_3b_CONVERTED.txt"):
            # Download the file
            match file_name:
                case 'dataset_4.csv':
                    data_set_4_extract()
                case 'dataset_5.xlsx':
                    data_set_5_extract()

            data_frame = pd.DataFrame(pd.read_csv(file_path))
            data_frame = data_frame.rename(columns=lambda x: x.replace('-', ''))
            return data_frame
        logging.error('Error: File not found')
    except FileNotFoundError:
        logging.error('Error: You do not have permission to access the file')
    return None

def manage_insert_to_db(dataset_name, data_frame):
    '''
    Insert dataset to DB
    '''
    create_table_if_needed(dataset_name, data_frame)
    truncate_table(dataset_name)
    insert_values(dataset_name, columns_to_str(data_frame.columns.tolist()), data_frame)

def create_table_if_needed(table_name, data_frame):
    '''
    Create a table with the specified columns and
    their corresponding types
    '''

    # Create a list with the names and types of the colums
    columns = []
    for col in data_frame.columns:
        if data_frame[col].dtype == 'object':
            columns.append([col, 'VARCHAR'])
        elif data_frame[col].dtype == 'int64':
            columns.append([col, 'BIGINT'])
        elif data_frame[col].dtype == 'float64':
            columns.append([col, 'FLOAT'])
        elif data_frame[col].dtype == 'datetime64[ns]':
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
        logging.error('Error: %s', error)
        conn.rollback()
        cursor.close()
        return 1

    logging.info("Table created successfully")
    cursor.close()
    return 0

def truncate_table(table):
    '''
    Truncate specified table
    '''
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
        logging.error('Error: %s', error)
        conn.rollback()
        cursor.close()
        return 1

    logging.info("The database has been truncated")
    cursor.close()
    return 0

def insert_values(table, cols, data):
    '''
    Insert dataframe into a given table with the specified columns
    '''
    # Connection to DBQ
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
        logging.error('Error: %s', error)
        conn.rollback()
        cursor.close()
        return 1

    logging.info("The dataset was inserted")
    cursor.close()
    return 0

def columns_to_str(data_frame_columns):
    '''
    Given the dataset columns, convert it
    to a comma separated string
    '''
    str_columns = ""
    for column in data_frame_columns:
        str_columns += column + ", "
    str_columns = str_columns[:-2]
    return str_columns

def extract_data_set(dataset_name):
    '''
    Call the specified function to retrieve the datasets from the web
    '''
    response = ""
    match dataset_name:
        case "dataset_1":
            response = data_set_1_extract()
        case "dataset_4":
            response = data_set_4_extract()
        case "dataset_5":
            response = data_set_5_extract()

    response['success'] = True
    return response

def to_db(dataset_name):
    '''
    Call the specified function to insert the values into the DB
    '''
    response = ""
    match dataset_name:
        case "dataset_1":
            response = data_set1_to_db()
        case "dataset_3a":
            response = data_set3a_to_db()
        case "dataset_3b":
            response = data_set3b_to_db()
        case "dataset_4":
            response = data_set4_to_db()
        case "dataset_5":
            response = data_set5_to_db()

    response['success'] = True
    return response
    