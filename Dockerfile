# Dockerfile for Python initialization
FROM python:3.11

# Set working directory
WORKDIR /app

# Copy requirements.txt first (to leverage Docker caching)
COPY scripts/requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your scripts folder into the container
COPY scripts/ ./scripts

# Copy .env file for environment variables
COPY ./.env ./

# Set default command to run your main.py script
CMD ["python", "scripts/main.py"]
