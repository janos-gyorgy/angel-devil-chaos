# Build on the homelab node:
#   cd chart/app/mini-api
#   sudo docker build -t mini-api:0.1.0 .
#   sudo docker save mini-api:0.1.0 | sudo k3s ctr images import -
# Built locally and imported into the node; no registry push.

FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

# Run as non-root.
RUN useradd -m -u 1000 app && chown -R app:app /app
USER app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
