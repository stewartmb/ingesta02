FROM python:3-slim
WORKDIR /app
COPY ingesta02.py .
RUN pip install mysql-connector-python boto3
CMD ["python", "ingesta02.py"]
