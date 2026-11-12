FROM python:3.11-slim

WORKDIR /app

# Primero las dependencias: esta capa queda en caché y solo se reconstruye
# cuando cambia requirements.txt, no en cada edición de consumer.py.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY consumer.py .

# -u desactiva el búfer de stdout; de lo contrario los logs no aparecerían en
# `docker logs` hasta que se llenara el búfer.
CMD ["python", "-u", "consumer.py"]
