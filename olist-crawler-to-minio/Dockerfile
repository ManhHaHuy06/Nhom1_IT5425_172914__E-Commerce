FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY crawler ./crawler
COPY raw ./raw
COPY logs ./logs
CMD ["python", "-m", "crawler.crawl"]
