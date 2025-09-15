# ---------- Base image ----------
FROM python:3.11-slim

# ---------- Set working directory ----------
WORKDIR /app

# ---------- Install system deps (librdkafka needed for confluent-kafka) ----------
RUN apt-get update && apt-get install -y \
    gcc \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# ---------- Copy requirements ----------
COPY requirements.txt .

# ---------- Install Python dependencies ----------
RUN pip install --no-cache-dir -r requirements.txt

# ---------- Copy source code ----------
COPY . .

# ---------- Run consumer ----------
CMD ["python", "consumer.py"]