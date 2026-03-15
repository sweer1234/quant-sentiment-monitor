FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt /app/requirements.txt
COPY requirements-dev.txt /app/requirements-dev.txt
COPY scripts/bootstrap_cloud_env.sh /app/scripts/bootstrap_cloud_env.sh
RUN bash /app/scripts/bootstrap_cloud_env.sh --quiet

COPY . /app

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

