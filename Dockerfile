# ------------------------------------------------------
# Base Image
# ------------------------------------------------------
FROM python:3.10-slim

# ------------------------------------------------------
# Set work directory
# ------------------------------------------------------
WORKDIR /app

# ------------------------------------------------------
# Install system dependencies
# ------------------------------------------------------
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# ------------------------------------------------------
# Copy Python dependencies first (better caching)
# ------------------------------------------------------
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

# ------------------------------------------------------
# Copy full project
# ------------------------------------------------------
COPY . /app

# ------------------------------------------------------
# Expose Streamlit port
# ------------------------------------------------------
EXPOSE 8080

# ------------------------------------------------------
# Streamlit environment configs
# ------------------------------------------------------
ENV STREAMLIT_SERVER_PORT=8080 \
    STREAMLIT_SERVER_ADDRESS="0.0.0.0" \
    STREAMLIT_SERVER_ENABLE_CORS=false \
    STREAMLIT_SERVER_ENABLE_XSRF_PROTECTION=false \
    STREAMLIT_BROWSER_GATHER_USAGE_STATS=false \
    PYTHONUNBUFFERED=1

# ------------------------------------------------------
# Run Streamlit app (main entry file)
# ------------------------------------------------------
CMD ["streamlit", "run", "frontend/Home.py", "--server.port=8080", "--server.address=0.0.0.0"]
