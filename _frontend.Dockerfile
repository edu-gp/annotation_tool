FROM alchemy-base

WORKDIR /app

EXPOSE 5001

COPY . .

ENV FLASK_APP frontend
ENV FLASK_ENV development
ENV FLASK_RUN_HOST 0.0.0.0
ENV FLASK_RUN_PORT 5001
CMD ["flask", "run"]