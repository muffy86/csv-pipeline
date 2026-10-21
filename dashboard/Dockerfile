FROM python:3.12-slim

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
COPY --chown=app:app . .

USER app
EXPOSE 8500
CMD ["python", "main.py"]
