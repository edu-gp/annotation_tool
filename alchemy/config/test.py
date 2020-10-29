from alchemy.config.base import *  # noqa # Must be absolute package name

DEBUG = True
TESTING = True
ALCHEMY_ENV = env('FLASK_ENV', default='test')

SQLALCHEMY_DATABASE_URI = "sqlite:///:memory:"

ANNOTATION_TOOL_ANNOTATION_SERVER_SECRET = 'asdasd'
ANNOTATION_TOOL_ADMIN_SERVER_PASSWORD = "password"

ALCHEMY_FILESTORE_DIR = env('ALCHEMY_FILESTORE_DIR', default='__filestore', cast=Path)
