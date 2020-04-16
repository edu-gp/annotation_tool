import os
basedir = os.path.abspath(os.path.dirname(__file__))


class Config(object):
    """Base config, uses staging database server."""
    DEBUG = False
    TESTING = False
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URL') or \
                              'sqlite:////annotation_tool/alchemy.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False


# TODO add a ProductionConfig


class DevelopmentConfig(Config):
    DEBUG = True


class TestingConfig(Config):
    DEBUG = True
    TESTING = True
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
