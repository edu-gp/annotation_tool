# Getting Started

Create an `.env` file or pass in your own environment vars to override configuration. See `.env.example`.

e.g.

```
env ANNOTATION_TOOL_BACKEND_PASSWORD=1234 flask run
```

Or use the --env or --env-file args for Docker.

Descriptions of the env vars:

- `ANNOTATION_TOOL_BACKEND_PASSWORD`: The password to login to the backend (admin dashboard).
- `ANNOTATION_TOOL_FRONTEND_SECRET`: The secret used to generate login links for annotators. If you change this, all login links will change.
- `ANNOTATION_TOOL_FRONTEND_SERVER`: URL of frontend server, e.g. `http://localhost:5001`.
- `ANNOTATION_TOOL_TASKS_DIR`: Where all the tasks and their related annotations and models are stored. Default is `./__tasks`
- `ANNOTATION_TOOL_DATA_DIR`: Where the raw data is stored. Default is `./__data`
- `ANNOTATION_TOOL_INFERENCE_CACHE_DIR`: Where some model inference are cached. Default is `./__infcache`
- `ANNOTATION_TOOL_MAX_PER_ANNOTATOR`: How many examples to assign to each annotator in a batch. Default is 100.
- `ANNOTATION_TOOL_MAX_PER_DP`: How many annotators should see the same example. Default is 3.
- `TRANSFORMER_MAX_SEQ_LENGTH`: Max sequence length - longer means more accurate models but longer training time and memory requirements. Setting to 128 is usually good enough for small machines. Default is 512.
- `TRANSFORMER_TRAIN_EPOCHS`: Default number of epochs to train. Default is 5.
- `TRANSFORMER_SLIDING_WINDOW`: If a sequence is too long (longer than `TRANSFORMER_MAX_SEQ_LENGTH`), should we use a sliding window to average out the result. Default is "True".
- `TRANSFORMER_TRAIN_BATCH_SIZE`: Training batch size. Default is 8.
- `TRANSFORMER_EVAL_BATCH_SIZE`: Prediction batch size. Default is 8.
- `GOOGLE_AI_PLATFORM_ENABLED`: Whether to use Google AI Platform for training. Default is False.
- `GOOGLE_AI_PLATFORM_BUCKET`: Distributed Training - bucket to store data.
- `GOOGLE_AI_PLATFORM_DOCKER_IMAGE_URI`: Distributed Training - pre-built training image URI.

# Exposing Port on Google Cloud

Add a Firewall group, then tag your instance with that group.

# Tests

```
pytest
```