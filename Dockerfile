FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y \
    python3-pip git curl wget gdal-bin libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

ENV GDAL_VERSION=3.8.4
WORKDIR /workspace
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

EXPOSE 8000 9090
ENTRYPOINT ["python", "main.py"]
CMD ["--mode", "serve", "--host", "0.0.0.0", "--port", "8000"]
