FROM python:3.11-slim

WORKDIR /app

# Install Python deps
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt \
    && python -c "import psycopg"

# Copy app code
COPY . /app

# Ensure expected folders exist (prevents /static missing crash)
RUN mkdir -p /app/static /data

# Use a persistent SQLite DB path (mount /data on Unraid)
ENV DATABASE_PATH=/data/reef.db

EXPOSE 8000

CMD ["sh", "-c", "pip install --no-cache-dir -r requirements.txt && uvicorn main:app --host 0.0.0.0 --port 8000"]
