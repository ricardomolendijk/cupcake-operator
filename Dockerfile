# Stage 1: Build dependencies
FROM python:3.13-slim AS builder

WORKDIR /app

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir --target=/app/dependencies -r requirements.txt

# Stage 2: Download kubectl
FROM alpine:3.19 AS kubectl-downloader

RUN apk add --no-cache curl && \
    curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl" && \
    chmod +x kubectl

# Stage 3: Runtime image
FROM python:3.13-slim

# Install only runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

WORKDIR /app

# Copy Python dependencies from builder
COPY --from=builder /app/dependencies /app/dependencies

# Copy kubectl binary
COPY --from=kubectl-downloader /kubectl /usr/local/bin/kubectl

# Copy application code
COPY . .

# Update PYTHONPATH for installed packages
ENV PYTHONPATH=/app/dependencies:$PYTHONPATH


# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

ENTRYPOINT ["python", "-u", "main.py"]
