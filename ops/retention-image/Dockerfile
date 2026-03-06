FROM postgres:16

WORKDIR /app

COPY retention.sql /app/retention.sql
COPY run_retention.sh /app/run_retention.sh

RUN chmod +x /app/run_retention.sh

CMD ["/app/run_retention.sh"]