FROM alpine:3.19

RUN apk add --no-cache bash curl postgresql15-client tzdata

WORKDIR /app
COPY docker/backup/backup.sh docker/backup/entrypoint.sh /app/
RUN chmod +x /app/backup.sh /app/entrypoint.sh

ENV TZ=Europe/Moscow \
    BACKUP_CRON_SCHEDULE="0 3 * * *"

CMD ["/app/entrypoint.sh"]
