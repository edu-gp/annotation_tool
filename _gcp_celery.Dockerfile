FROM alchemy-base

WORKDIR /app

COPY . .

CMD ["celery", "--app=train.gcp_celery", "worker", "-Q", "gcp_celery", "-c", "5", "--autoscale=100,2", "-l", "info", "-n", "gcp_celery"]
