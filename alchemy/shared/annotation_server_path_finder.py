from werkzeug.urls import url_encode

from alchemy.shared.config import Config


def generate_annotation_server_admin_examine_path(task_id, username):
    return f"/tasks/{task_id}/examine/{username}"


def generate_annotation_server_compare_path(task_id, label, users_dict=None):
    params = {"label": label}
    if users_dict:
        params.update(users_dict)
    return f"/tasks/{task_id}/compare?" + url_encode(params)


def generate_annotation_server_admin_examine_link(task_id, username):
    return (
        f"{Config.get_annotation_server()}"
        f"{generate_annotation_server_admin_examine_path(task_id, username)}"
    )


def generate_annotation_server_compare_link(task_id, label, users_dict=None):
    return (
        f"{Config.get_annotation_server()}"
        f"{generate_annotation_server_compare_path(task_id, label, users_dict)}"
    )
