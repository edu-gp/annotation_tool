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

# Exposing Port on Google Cloud

Add a Firewall group, then tag your instance with that group.

# Tests

```
pytest
```