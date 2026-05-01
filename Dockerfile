# 1 Python version
FROM python:3.11-slim

# 2 Working directory
WORKDIR /app

# 3. Install PostgreSQL dependencies
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 4. copy requirements.txt to the working directory
COPY requirements.txt .

# 5 Install Python dependencies
RUN pip install -r requirements.txt

# 6 Copy the source code into the container
COPY src/ ./src/

# 7 Create a data directory for any temporary files if needed
RUN mkdir -p /app/data

CMD ["python"]