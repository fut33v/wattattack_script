FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY . .

ENV WATTATTACK_ACCOUNTS_FILE=/app/accounts.json \
    WATTATTACK_HTTP_TIMEOUT=30 \
    WATTATTACK_RECENT_LIMIT=5

CMD ["python", "wattattack_bot.py"]
