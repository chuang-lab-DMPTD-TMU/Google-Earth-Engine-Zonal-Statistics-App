FROM python:3.10-slim

# Install system dependencies for geospatial libraries and visualization
RUN apt-get update && apt-get install -y \
    build-essential \
    libgdal-dev \
    libproj-dev \
    gdal-bin \
    graphviz \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Expose Streamlit's default port
EXPOSE 8501

# Command to run your app
CMD ["streamlit", "run", "main.py", "--server.address=0.0.0.0", "--server.port=8501"]