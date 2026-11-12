FROM python:3.11-slim

WORKDIR /app

# Primero las dependencias, para que esta capa siga en caché al editar el código.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

EXPOSE 8000

# --host 0.0.0.0 es obligatorio: el 127.0.0.1 por defecto de uvicorn solo
# escucharía dentro del contenedor y el puerto publicado nunca respondería.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
