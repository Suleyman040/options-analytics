FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install .

# Don't run as root inside the container.
RUN useradd --create-home app
USER app

EXPOSE 8000
CMD ["uvicorn", "options_analytics.api:app", "--host", "0.0.0.0", "--port", "8000"]
