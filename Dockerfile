FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY pyproject.toml README.md ./
COPY src/ ./src/

RUN python -m pip install --no-cache-dir .

EXPOSE 8000 8001

CMD ["uvicorn", "mobility.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
