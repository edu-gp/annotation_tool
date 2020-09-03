FROM alchemy-base

WORKDIR /app

COPY . .

CMD ["celery", "--app=train.train_celery", "worker", "-Q", "train_celery", "-c", "2", "--autoscale=100,2", "-l", "info", "--max-tasks-per-child", "1", "-n", "train_celery"]
