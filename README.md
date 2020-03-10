# Getting Started

Create an `.env` file or pass in your own environment vars to override configuration. See `.env.example`.

e.g.

```
env ANNOTATION_TOOL_BACKEND_PASSWORD=1234 flask run
```

Or use the --env or --env-file args for Docker.

# Tests

```
pytest
```