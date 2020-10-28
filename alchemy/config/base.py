import os
from envparse import env
from alchemy.admin_server.external_services import SecretManagerService
from pathlib import Path


basedir = os.path.abspath(os.path.dirname(__file__))


"""Base config, uses staging database server."""
DEBUG = False
TESTING = False

SQLALCHEMY_DATABASE_URI = env(  # maybe env.url?
    'ALCHEMY_DATABASE_URI', default='sqlite:////annotation_tool/alchemy.db')
SQLALCHEMY_TRACK_MODIFICATIONS = False

SECRET_KEY="athena_todo_change_this_in_prod"

ANNOTATION_SERVER_LOGGER = env("ANNOTATION_SERVER_LOGGER", default="alchemy-annotation-server")
ADMIN_SERVER_LOGGER = env("ADMIN_SERVER_LOGGER", default="alchemy-admin-server")

USE_CLOUD_LOGGING = env.bool("USE_CLOUD_LOGGING", default=False)
GOOGLE_AI_PLATFORM_ENABLED = env.bool('GOOGLE_AI_PLATFORM_ENABLED', default=False)
GCP_PROJECT_ID = env("GCP_PROJECT_ID", default=None)

ANNOTATION_TOOL_MAX_PER_ANNOTATOR = env.int('ANNOTATION_TOOL_MAX_PER_ANNOTATOR', default=100)
ANNOTATION_TOOL_MAX_PER_DP = env.int('ANNOTATION_TOOL_MAX_PER_DP', default=3)
ALCHEMY_FILESTORE_DIR = env('ALCHEMY_FILESTORE_DIR', cast=Path, default='__filestore')

API_TOKEN = (env('API_TOKEN', None) or
                SecretManagerService.get_secret(
                    project_id=GCP_PROJECT_ID,
                    secret_id=env("API_TOKEN_NAME")
                ))
