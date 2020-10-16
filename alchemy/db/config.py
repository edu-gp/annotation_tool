import os
from envparse import env
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    """Base config, uses staging database server."""
    DEBUG = False
    TESTING = False
    SQLALCHEMY_DATABASE_URI = env(  # maybe env.url?
        'ALCHEMY_DATABASE_URI', default='sqlite:////annotation_tool/alchemy.db')
    SQLALCHEMY_TRACK_MODIFICATIONS = False


# TODO add a ProductionConfig


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
