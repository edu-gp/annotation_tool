import os

from envparse import env


class Config:
    """
    Set configuration vars from .env file.

    This module is designed to fail loudly when a required env var is not found.
    """

    @staticmethod
    def get_annotation_server():
        '''e.g. http://localhost:5001'''
        return env('ANNOTATION_TOOL_ANNOTATION_SERVER_SERVER')

    @staticmethod
    def get_inference_cache_dir():
        return env('ANNOTATION_TOOL_INFERENCE_CACHE_DIR', default=os.path.join(os.getcwd(), '__infcache'))
