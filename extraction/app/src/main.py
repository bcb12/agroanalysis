'''Import declarations'''
from fastapi import FastAPI
from extraction import extract_data_set
from extraction import to_db

app = FastAPI()

@app.get("/extraction/extract/{ds_name}")
def ds_extract(ds_name: str):
    '''
    Endpoint which takes a dataset name as input
    and retrieves it from the web into the database
    '''
    return {"response": extract_data_set(ds_name)}

@app.get("/extraction/todb/{ds_name}")
def ds_todb(ds_name: str):
    '''
    Endpoint which takes a dataset name as input
    to insert it from the file to the database
    '''
    return {"response": to_db(ds_name)}
