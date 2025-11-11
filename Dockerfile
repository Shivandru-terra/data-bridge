# Use a lightweight Python image
FROM python:3.11-slim

# Ensure logs show up immediately
ENV PYTHONUNBUFFERED=True

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential && \
    rm -rf /var/lib/apt/lists/*

# Set the working directory
WORKDIR /app

# Copy dependencies file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy all source code
COPY . .

# Expose the Cloud Run port
ENV PORT=8080

# Run the FastAPI app with Uvicorn
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]