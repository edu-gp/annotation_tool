FROM alchemy-base

WORKDIR /app

COPY . .

CMD ["celery", "--app=ar.ar_celery", "worker", "-Q", "ar_celery", "-c", "2", "--autoscale=100,2", "-l", "info", "--max-tasks-per-child", "1", "-n", "ar_celery"]
