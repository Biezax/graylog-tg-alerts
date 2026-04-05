FROM python:3.14.3-slim-bookworm

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app/ .
COPY config/ ./config/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
