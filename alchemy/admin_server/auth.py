from alchemy.shared.config import Config
from flask_httpauth import HTTPBasicAuth

auth = HTTPBasicAuth()


@auth.verify_password
def verify_password(username, password):
    return password == Config.get_admin_server_password()
