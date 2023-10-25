FROM python:3.10

WORKDIR /app

COPY requirements.txt .

RUN pip install -U pip && \
    pip install -r requirements.txt

COPY . .

CMD ["python3", "-u", "./src/job.py"]