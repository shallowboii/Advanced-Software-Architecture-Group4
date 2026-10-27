FROM python:3.13.5-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PORT=8081 INVENTORY_STATE_FILE=/data/inventory.json
RUN groupadd --gid 10001 inventory && useradd --uid 10001 --gid inventory --no-create-home inventory \
    && mkdir -p /app /data && chown inventory:inventory /app /data
WORKDIR /app
COPY --chown=inventory:inventory src/inventory/server.py ./server.py
USER inventory
EXPOSE 8081
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8081/health',timeout=2)"
CMD ["python", "server.py"]
