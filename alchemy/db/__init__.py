from alchemy.shared.config import Config


def _data_dir():
    d = Config.get_data_dir()
    d.mkdir(parents=True, exist_ok=True)
    return str(d)


def _task_dir(task_id=None):
    d = Config.get_tasks_dir()
    d.mkdir(parents=True, exist_ok=True)
    if task_id:
        d = d / task_id
    return str(d)
