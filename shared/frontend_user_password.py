from shared.config import Config

import hashlib


def get_frontend_user_password(username):
    '''
    Used for generating log-in links for users
    '''
    s = f'{username}:{Config.get_frontend_secret()}'
    return hashlib.sha256(s.encode()).hexdigest()


def _generate_frontend_user_login_path(username):
    password = get_frontend_user_password(username)
    return f'/auth/login?username={username}&password={password}'


def generate_frontend_admin_examine_path(task_id, username):
    return f'/tasks/{task_id}/examine/{username}'


def generate_frontend_user_login_link(username):
    return f'{Config.get_frontend_server()}{_generate_frontend_user_login_path(username)}'


def generate_frontend_admin_examine_link(task_id, username):
    return f'{Config.get_frontend_server()}' \
           f'{generate_frontend_admin_examine_path(task_id, username)}'
