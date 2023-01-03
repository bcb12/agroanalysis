from fastapi import FastAPI, Request, Form
from fastapi.staticfiles import StaticFiles
import jinja2
from fastapi.templating import Jinja2Templates
import pandas as pd
from serve import get_dataset

app = FastAPI()
templates = Jinja2Templates(directory="templates/")
app.mount("/static", StaticFiles(directory="static/"), name="static")

@app.get("/serve/home")
def form_post(request: Request):
    result = "Indica el dataset"
    return templates.TemplateResponse('index.html', context={'request': request, 'result': result})

@app.get("/serve/{dataset_name}")
def form_post(request: Request, dataset_name):
    result = get_dataset(dataset_name)
    columns = list(result.columns)
    data = result.values.tolist()
    return templates.TemplateResponse("dsview.html", context={'request': request, "dataset_name": dataset_name, "columns": columns, "data": data})
