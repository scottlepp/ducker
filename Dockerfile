FROM --platform=linux/amd64 python:3.11.3

WORKDIR /usr/src/app

RUN pip install --upgrade pip setuptools

RUN pip install openai duckdb tabulate numpy pandas --no-cache-dir 

RUN pip install flask waitress --no-cache-dir 

CMD [ "python", "./app.py" ]