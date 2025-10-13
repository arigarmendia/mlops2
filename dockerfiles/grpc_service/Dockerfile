FROM python:3.11-slim

WORKDIR /app

# Install system dependencies including net-tools for healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application code first
COPY . .

# Compile proto files
RUN python -m grpc_tools.protoc \
    -I./proto \
    --python_out=. \
    --grpc_python_out=. \
    proto/prediction.proto

# Expose gRPC port
EXPOSE 50051

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD ["sh", "-c", "netstat -tuln | grep 50051 || exit 1"]

# Run gRPC server
CMD ["python", "main.py"]
