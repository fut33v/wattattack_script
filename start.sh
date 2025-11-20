#!/bin/bash
# Скрипт для сборки и запуска контейнеров с отметкой времени

echo "🛠  Building and starting containers..."
docker compose up -d --build --remove-orphans

if [ $? -eq 0 ]; then
    echo "🚀 Containers up and running at $(date '+%Y-%m-%d %H:%M:%S')"
else
    echo "❌ Build or startup failed at $(date '+%Y-%m-%d %H:%M:%S')"
fi

