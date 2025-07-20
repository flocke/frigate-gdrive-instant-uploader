FROM python:3.8

RUN apt-get update && \
    apt-get install -y nano sqlite3

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "main.py"]
