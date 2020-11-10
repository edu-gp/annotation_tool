from pathlib import Path
from typing import Union, Optional

from envparse import env

RAW_DATA_DIR = "raw_data"
TRAINING_DATA_DIR = "training_data"
MODELS_DIR = "models"

PathT = Union[Path, str]


def filestore_base_dir() -> str:
    return env('ALCHEMY_FILESTORE_DIR', default='__filestore')


def _check_base(base: Optional[PathT]) -> Path:
    if base is None:
        # I'm not checking for `if not base:`, since  bool('' is None) == False
        # but bool(not '') == True
        base = filestore_base_dir()
    if not isinstance(base, Path):
        base = Path(base)
    return base


def _ensure_return_type(p: PathT, as_path: bool) -> PathT:
    if as_path and not isinstance(p, Path):
        return Path(p)
    elif not as_path and isinstance(p, Path):
        return str(p)

    return p


def _make_path(base: Optional[PathT], as_path: bool, directory: str) -> PathT:
    base = _check_base(base)
    return _ensure_return_type(base / directory, as_path)


def raw_data_dir(base: Optional[PathT] = None, as_path: bool = False) -> PathT:
    return _make_path(base, as_path, RAW_DATA_DIR)


def models_dir(base: Optional[PathT] = None, as_path: bool = False) -> PathT:
    return _make_path(base, as_path, MODELS_DIR)


def training_data_dir(base: Optional[PathT] = None, as_path: bool = False) -> PathT:
    return _make_path(base, as_path, TRAINING_DATA_DIR)


def bucket_name():
    return 'alchemy-staging'
