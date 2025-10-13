FROM python:3.10-slim

WORKDIR /app

COPY . .
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

EXPOSE 8801

CMD ["uvicorn", "app_graphql:app", "--host", "0.0.0.0", "--port", "8801"]
