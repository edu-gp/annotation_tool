import os
from pathlib import Path

from envparse import env

from alchemy.admin_server.external_services import SecretManagerService

basedir = os.path.abspath(os.path.dirname(__file__))


"""Base config, uses staging database server."""
DEBUG = False
TESTING = False
ALCHEMY_ENV = env('FLASK_ENV', default='development')

SQLALCHEMY_DATABASE_URI = env(  # maybe env.url?
    'ALCHEMY_DATABASE_URI', default='sqlite:////annotation_tool/alchemy.db')
SQLALCHEMY_TRACK_MODIFICATIONS = False

SECRET_KEY="athena_todo_change_this_in_prod"

ANNOTATION_SERVER_LOGGER = env("ANNOTATION_SERVER_LOGGER", default="alchemy-annotation-server")
ADMIN_SERVER_LOGGER = env("ADMIN_SERVER_LOGGER", default="alchemy-admin-server")

USE_CLOUD_LOGGING = env.bool("USE_CLOUD_LOGGING", default=False)
GOOGLE_AI_PLATFORM_ENABLED = env.bool('GOOGLE_AI_PLATFORM_ENABLED', default=False)
GOOGLE_AI_PLATFORM_BUCKET = env('GOOGLE_AI_PLATFORM_BUCKET', default=None)
GCP_PROJECT_ID = env("GCP_PROJECT_ID", default=None)
GCP_API_TOKEN_NAME = env("API_TOKEN_NAME", default=None)

ANNOTATION_TOOL_MAX_PER_ANNOTATOR = env.int('ANNOTATION_TOOL_MAX_PER_ANNOTATOR', default=100)
ANNOTATION_TOOL_MAX_PER_DP = env.int('ANNOTATION_TOOL_MAX_PER_DP', default=3)
ALCHEMY_FILESTORE_DIR = env('ALCHEMY_FILESTORE_DIR', cast=Path, default='__filestore')

API_TOKEN = (env('API_TOKEN', None) or
                SecretManagerService.get_secret(
                    project_id=GCP_PROJECT_ID,
                    secret_id=GCP_API_TOKEN_NAME,
                ))


# train/prep.py
TRAINING_CONFIG = {
    'num_train_epochs': env.int("TRANSFORMER_TRAIN_EPOCHS", default=5),
    'sliding_window': env.bool("TRANSFORMER_SLIDING_WINDOW", default=True),
    'max_seq_length': env.int("TRANSFORMER_MAX_SEQ_LENGTH", default=512),
    'train_batch_size': env.int("TRANSFORMER_TRAIN_BATCH_SIZE", default=8),
    # NOTE: Specifying a large batch size during inference makes the
    # process take up unnessesarily large amounts of memory.
    # We'll only toggle this on at inference time.
    # 'eval_batch_size': env.int("TRANSFORMER_EVAL_BATCH_SIZE", default=8),
}
