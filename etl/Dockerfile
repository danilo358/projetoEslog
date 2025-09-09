FROM python:3.11-slim

ENV TZ=America/Campo_Grande

WORKDIR /app
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY etl.py config.yml ./

CMD ["python", "etl.py"]
