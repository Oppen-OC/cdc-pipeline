FROM python:3.11-slim

WORKDIR /app

# Dependencies first: this layer is cached and only rebuilds when
# requirements.txt changes, not on every edit of consumer.py.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

# -u disables stdout buffering, otherwise the logs would not show up in
# `docker logs` until the buffer fills.
CMD ["python", "-u", "consumer.py"]
