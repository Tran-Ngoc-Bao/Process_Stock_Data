ARG SUPERSET_IMAGE_VERSION
FROM apache/superset:${SUPERSET_IMAGE_VERSION}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc \
  && rm -rf /var/lib/apt/lists/*

USER superset
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY docker-entrypoint-init.sh /app/docker-entrypoint-init.sh
USER root
RUN chmod +x /app/docker-entrypoint-init.sh
USER superset

ENTRYPOINT ["/app/docker-entrypoint-init.sh"]