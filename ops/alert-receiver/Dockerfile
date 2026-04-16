FROM python:3.13-slim

RUN apt-get update \
 && apt-get upgrade -y \
 && rm -rf /var/lib/apt/lists/*

RUN groupadd -r appuser && useradd -r -g appuser -d /app -s /sbin/nologin appuser

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && pip install --no-cache-dir --upgrade \
      setuptools wheel urllib3 "jaraco.context"

COPY app.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 5000

USER appuser
CMD ["python", "app.py"]
