FROM python:3.11-slim

WORKDIR /app
COPY requirements.ml-worker.txt ./
RUN pip install --no-cache-dir -r requirements.ml-worker.txt

COPY ml ./ml

CMD ["python", "-m", "ml.worker"]
