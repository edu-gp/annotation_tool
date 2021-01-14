from datetime import datetime

from alchemy.db.fs import raw_data_dir
from alchemy.db.utils import get_all_data_files


def check_file_system() -> bool:
    # Check read
    try:
        get_all_data_files()
    except:
        return False

    # Check write
    f = (raw_data_dir(as_path=True) / datetime.utcnow().strftime("fs-health-check-%Y%m%d%H%M%S.txt"))
    try:
        f.write_text("test", encoding='utf-8', errors='ignore')
    except:
        return False
    finally:
        try:
            f.unlink()
        except FileNotFoundError as ignored:  # in python 3.8+ you can pass missing_ok=True
            pass

    return True


def check_celery() -> bool:
    try:
        from alchemy.ar.ar_celery import hello
        task_result = hello.apply_async()
        task_result.wait(timeout=5)
        assert task_result.successful()
    except:
        return False
    else:
        return True
