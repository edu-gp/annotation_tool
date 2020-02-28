
# (Optional) Build

```
docker build -t myapp .
```

# Start Docker

```
docker run --gpus all -d -v ~/annotation_tool:/annotation_tool -p 5000:5000 -p 5001:5001 -p 9001:9001 myapp bash /annotation_tool/docker/run.sh
```

# Tunnel ports

```
ssh -i ~/.ssh/google_compute_engine -N -L 9001:localhost:9001 -L 5000:localhost:5000 -L 5001:localhost:5001 eddiedu@35.223.18.100
```

# Move data in there

```
cd __data
sudo wget https://storage.googleapis.com/nlp-flywheel-data/healthcare_patterns.jsonl
sudo wget https://storage.googleapis.com/nlp-flywheel-data/spring_jan_2020_small.jsonl
```

# Inspect logs

```
docker ps    # <- get the CONTAINER_ID

docker exec -it <CONTAINER_ID> bash

supervisorctl tail -f train_celery stderr
```