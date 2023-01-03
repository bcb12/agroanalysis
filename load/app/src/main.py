from fastapi import FastAPI
from load import clean_dataset
from load import generate_dataset_final

app = FastAPI()

@app.get("/load/clean/{ds}")
def clean_ds(ds: str):
    return {"response": clean_dataset(ds)}

@app.get("/load/gen_final_ds")
def gen_final_ds():
     return {"response": generate_dataset_final()}
