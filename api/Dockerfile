FROM python:3.11-slim

WORKDIR /app

# Dependencies first so this layer stays cached across code edits.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

# --host 0.0.0.0 is mandatory: uvicorn's default 127.0.0.1 would only listen
# inside the container and the published port would never answer.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
