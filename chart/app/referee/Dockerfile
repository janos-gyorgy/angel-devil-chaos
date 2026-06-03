# Build on the node:
#   cd chart/app/referee
#   sudo docker build -t da-referee:0.1.0 .
#   sudo docker save da-referee:0.1.0 | sudo k3s ctr images import -

FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY templates ./templates

ENV PYTHONUNBUFFERED=1
EXPOSE 8080

RUN useradd -m -u 1000 app && chown -R app:app /app
USER app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
