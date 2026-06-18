FROM python:3.11-slim

WORKDIR /app

# Native indicators need a compiler and CMake during image build.
# We compile once here so the runtime container only needs to import the module.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        cmake \
    && rm -rf /var/lib/apt/lists/*

# The worker only needs the small runtime set for Sprint 2.
# We do not install the full ML stack here because that would pull huge Torch/CUDA packages.
COPY requirements.ml-worker.txt ./
RUN pip install --no-cache-dir -r requirements.ml-worker.txt

# Copy the worker code and native source before running the shared build script.
COPY ml ./ml
COPY scripts/build_indicators.sh ./scripts/build_indicators.sh
RUN ./scripts/build_indicators.sh

# Start the Kafka consumer.
CMD ["python", "-m", "ml.worker"]
