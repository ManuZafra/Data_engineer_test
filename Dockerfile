# Imagen base oficial de Python
FROM python:3.9-slim

# Directorio de trabajo en el contenedor
WORKDIR /app

# Copiar requirements.txt (lo crearemos después)
COPY requirements.txt .

# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el resto de los scripts
COPY scripts/ .

# Comando por defecto (ejecutar un script hello-world)
CMD ["python", "hello_world.py"]
