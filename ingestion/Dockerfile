# -------------------------
# Base image
# -------------------------
FROM python:3.13-slim

# -------------------------
# Environment config
# -------------------------
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# -------------------------
# System dependencies
# -------------------------
RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# -------------------------
# Working directory
# -------------------------
WORKDIR /app

# -------------------------
# Python dependencies
# -------------------------
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# -------------------------
# Application code
# -------------------------
COPY . .

# -------------------------
# Create logs directory
# -------------------------
RUN mkdir -p ingestion/logs

# -------------------------
# Default command
# -------------------------
CMD ["python", "-m", "ingestion.ingest_all"]