FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

# Install required packages
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    ca-certificates \
    unixodbc \
    unixodbc-dev \
    libgssapi-krb5-2 \
    build-essential && \
    mkdir -p /etc/apt/keyrings

# Add Microsoft signing key and ODBC repo (for Debian 11 / Bullseye)
RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && ACCEPT_EULA=Y apt-get install -y msodbcsql18

# Install bash and ping for debugging
RUN apt-get install -y iputils-ping bash

WORKDIR /app
# Update this line:
COPY backend/requirements.txt ./requirements.txt
COPY backend/app /app/app
RUN pip install --no-cache-dir -r requirements.txt

ENV PYTHONPATH=/app

CMD ["python", "-m", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]