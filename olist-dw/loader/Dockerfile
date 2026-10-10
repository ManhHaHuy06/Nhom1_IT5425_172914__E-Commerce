FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN python -m pip install --upgrade pip --retries 5 --timeout 120
RUN python -m pip install --no-cache-dir --index-url https://pypi.org/simple --retries 5 --timeout 120 -r requirements.txt


COPY loader.py /app/loader.py

CMD ["python", "/app/loader.py"]
