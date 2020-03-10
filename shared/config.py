from os import environ

class Config:
    """Set configuration vars from .env file."""

    def get_backend_password():
        return environ.get('ANNOTATION_TOOL_BACKEND_PASSWORD')
