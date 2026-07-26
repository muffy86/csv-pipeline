FROM python:3.14-slim

WORKDIR /app

RUN pip install --no-cache-dir pandas

COPY pipeline.py .

CMD ["python", "pipeline.py"]
