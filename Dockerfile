FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
COPY kafka_tester ./kafka_tester

# Numeric, so a pod with runAsNonRoot can verify it without setting runAsUser.
USER 10001:10001
EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=4)"]

# gthread workers keep a slow Kafka test from blocking the next request, and
# /dev/shm holds gunicorn's worker heartbeat files so the root filesystem can be
# read-only. GUNICORN_CMD_ARGS adds to or overrides any of this at run time.
CMD ["gunicorn", "--bind", "0.0.0.0:8000", "--workers", "2", "--threads", "4", \
     "--worker-tmp-dir", "/dev/shm", "--access-logfile", "-", \
     "kafka_tester.app:create_app()"]
