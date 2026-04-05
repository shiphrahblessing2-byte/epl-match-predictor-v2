FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy everything needed
COPY models/  ./models/
COPY src/     ./src/
COPY data/    ./data/

# HuggingFace Spaces requires port 7860
EXPOSE 7860

CMD ["uvicorn", "src.api:app", "--host", "0.0.0.0", "--port", "7860"]