FROM python:3.12.3

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

COPY ingestion_service/ ./ingestion_service/
COPY .env.example .env

RUN useradd -m -u 1000 ingestion && chown -R ingestion:ingestion /app
USER ingestion

HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD python -c "import asyncio; asyncio.run(__import__('ingestion_service.main').main())" || exit 1

CMD ["python", "-m", "ingestion_service.main"]