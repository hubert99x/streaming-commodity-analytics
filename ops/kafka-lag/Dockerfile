FROM python:3.12-slim

RUN apt-get update \
 && apt-get upgrade -y \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir --upgrade \
      setuptools wheel urllib3 "jaraco.context"

COPY kafka_lag.py .

ENV PYTHONUNBUFFERED=1

USER appuser
CMD ["python", "kafka_lag.py"]