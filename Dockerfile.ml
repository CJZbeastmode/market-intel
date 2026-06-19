FROM python:3.11-slim

WORKDIR /app

# Native indicators need a compiler and CMake during image build.
# We compile once here so the runtime container only needs to import the module.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
    && rm -rf /var/lib/apt/lists/*

# The worker installs only the runtime stack needed so far:
# Kafka jobs, DB/Redis clients, C++ indicators, and the Prophet baseline.
# Keep heavy deep-learning stacks out of this image so the normal worker stays lean.
COPY requirements.ml-worker.txt ./
RUN pip install --no-cache-dir -r requirements.ml-worker.txt

# Copy the worker code and native source before running the shared build script.
# COPY ml brings in jobs, DB helpers, and the Sprint 4 prediction model code too.
COPY ml ./ml
COPY scripts/build_indicators.sh ./scripts/build_indicators.sh
RUN ./scripts/build_indicators.sh

# Start the Kafka consumer.
CMD ["python", "-m", "ml.worker"]
