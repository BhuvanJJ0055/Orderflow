FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends libpq-dev gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install --no-cache-dir streamlit==1.40.2 plotly==5.24.1 altair==5.4.1

COPY src/    src/
COPY dashboard/ dashboard/
COPY .env.example .env.example

ENV PYTHONPATH=/app

EXPOSE 8501
CMD ["streamlit", "run", "dashboard/app.py", \
     "--server.port=8501", "--server.address=0.0.0.0", \
     "--server.headless=true"]
