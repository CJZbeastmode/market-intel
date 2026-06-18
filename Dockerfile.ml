FROM python:3.11-slim

WORKDIR /app
# The worker only needs the small runtime set for Sprint 2.
# We do not install the full ML stack here because that would pull huge Torch/CUDA packages.
COPY requirements.ml-worker.txt ./
RUN pip install --no-cache-dir -r requirements.ml-worker.txt

# Copy only the Python worker code.
COPY ml ./ml

# Start the Kafka consumer.
CMD ["python", "-m", "ml.worker"]
