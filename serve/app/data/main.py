from fastapi import FastAPI
import jinja2
from fastapi.templating import Jinja2Templates
import pandas as pd
from serve import get_ds

app = FastAPI()
templates = Jinja2Templates(directory="templates/")

@app.get("/test")
def form_post(request: Request):
    result = "Indica el dataset"
    return templates.TemplateResponse('index.html', context={'request': request, 'result': result})


@app.post("/test")
def form_post(request: Request, dataset: str = Form(...)):
    result = get_ds(dataset)
    columns = list(result.columns)
    data = result.values.tolist()
    return templates.TemplateResponse("dsview.html", context={'request': request, "columns": columns, "data": data})

@app.get("/getds/{ds}")
def get_ds(ds: str):
    result = get_ds(ds)
    return result.to_dict()

@app.get("/raw_ds1")
def raw_ds1():
    df = get_ds('dataset_1')
    return df.to_html()
