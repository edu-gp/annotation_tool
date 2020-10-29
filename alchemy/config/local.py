from alchemy.config.base import *  # noqa # Must be absolute package name

DEBUG = True
ALCHEMY_ENV = env('FLASK_ENV', default='development')

USE_CLOUD_LOGGING = False
GOOGLE_AI_PLATFORM_ENABLED = False
