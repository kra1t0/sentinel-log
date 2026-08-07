FROM python:3.12-slim
WORKDIR /app

ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update && apt-get install -y python3-tk && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
CMD ["uvicorn", "app.app:app", "--host", "0.0.0.0", "--port", "8000"]
