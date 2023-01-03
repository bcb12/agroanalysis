# 
FROM python:3.11

# 
WORKDIR /app/

# 
COPY ./requirements.txt .

# 
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# 
COPY . /app/

#
EXPOSE 88

#
CMD ["uvicorn", "--app-dir", "src", "main:app", "--reload", "--host", "0.0.0.0", "--port", "88"]