FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY etl/ ./etl/
COPY database/ ./database/

ENV PYTHONPATH=/app

CMD ["tail", "-f", "/dev/null"]