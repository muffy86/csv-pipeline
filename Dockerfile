FROM python:3.14-slim

RUN apt-get update \
    && apt-get upgrade -y \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
WORKDIR /app

RUN groupadd --system app && useradd --system --gid app --home-dir /app app
COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir --disable-pip-version-check -r requirements.txt
COPY --chown=app:app pipeline.py ./
RUN mkdir -p /app/incoming /app/processed /app/archive && chown -R app:app /app

USER app
CMD ["python", "pipeline.py"]
