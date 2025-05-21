import datetime
import json
import os
import os.path as osp
import pickle
from typing import Any, Union


def load_pickle(file_path: str) -> Any:
    """Load a pickle file.

    First tries to load the pickle file with the default encoding (utf-8), if that fails, it tries
    it again with 'latin1' encoding.

    Args:
        file_path: path to the pickle file

    Returns:
        The object loaded from in the pickle file
    """
    with open(file_path, 'rb') as f:
        try:
            try:
                return pickle.load(f)
            except UnicodeDecodeError:
                f.seek(0)
                return pickle.load(f, encoding='latin1')
        except:
            print(f"Pickle file {file_path} cannot be loaded")
            raise


def dump_pickle(data: Any, file_path: str, protocol: int = pickle.DEFAULT_PROTOCOL):
    """Dump data to a pickle file.

    If the parent directory of the file does not exist, it will be created.
    If the file already exists, it will be overwritten.

    Args:
        data: data to be pickled
        file_path: path to the destination pickle file
        protocol: pickle protocol to use
    """
    ensure_parent_dir_exists(file_path)
    with open(file_path, 'wb') as f:
        pickle.dump(data, f, protocol)


def dump_json(data, path, **kwargs):
    """Dump data to a json file.

    If the parent directory of the file does not exist, it will be created.
    If the file already exists, it will be overwritten.

    Args:
        data: data to be dumped
        path: path to the destination json file
        **kwargs: additional arguments to be passed to ``json.dump``
    """
    ensure_parent_dir_exists(path)
    with open(path, 'w') as file:
        return json.dump(data, file, **kwargs)


def load_yaml(path: str):
    """Load data from a yaml file.

    Args:
        path: path to the yaml file

    Returns:
        The data loaded from the yaml file
    """
    import yaml

    with open(path) as file:
        return yaml.safe_load(file)


def write_file(content, path, is_binary=False):
    """Write content to a new file.

    If the parent directory of the file does not exist, it will be created.
    If the file already exists, it will be overwritten.

    Args:
        content: content to be written. If is_binary is False, it will be converted to a string.
        path: path to the destination file
        is_binary: if True, the content will be written as binary data, otherwise as text
    """
    mode = 'wb' if is_binary else 'w'
    ensure_parent_dir_exists(path)
    with open(path, mode) as f:
        if not is_binary:
            content = str(content)
        f.write(content)


def ensure_parent_dir_exists(filepath):
    """Ensure that the parent directory of a file path exists by creating any needed directories."""
    os.makedirs(osp.dirname(filepath), exist_ok=True)


def read_file(path: str, is_binary: bool = False) -> Union[str, bytes]:
    """Read the content of a file.

    Args:
        path: path to the file
        is_binary: if True, the content will be read as binary data, otherwise as text

    Returns:
        The content of the file
    """
    mode = 'rb' if is_binary else 'r'
    with open(path, mode) as f:
        return f.read()


def read_lines(path: str) -> list[str]:
    """Read the lines of a file.

    Splits the content as str.splitlines() does.

    Args:
        path: path to the file

    Returns:
        The lines of the file as a list of strings.

    """
    return read_file(path).splitlines()


def load_json(path):
    """Load data from a json file.

    Args:
        path: path to the json file

    Returns:
        The data loaded from the json file
    """
    with open(path) as file:
        return json.load(file)


def is_pickle_readable(p):
    """Check if a pickle file can be read.

    Tries to read the pickle file and returns True if it succeeds, False otherwise.
    The reason for the failure may be that the file does not exist, is corrupted, or the data
    cannot be unpickled for some other reason.

    Args:
        p: path to the pickle file

    Returns:
        True if the pickle file can be read, False otherwise
    """
    try:
        load_pickle(p)
        return True
    except Exception:
        return False


def is_file_newer(path: str, min_time: str = None) -> bool:
    """Check if a file exists and is newer than a given time.

    Args:
        path: path to the file
        min_time: minimum time the file must have been modified after. If None, only existence is
            checked. If a string, it must be in the format 'YYYY-MM-DDTHH:MM:SS'.

    Returns:
         True if the file exists and is newer than min_time, False otherwise
    """
    if min_time is None:
        return osp.exists(path)
    min_time = datetime.datetime.strptime(min_time, '%Y-%m-%dT%H:%M:%S').timestamp()
    return osp.exists(path) and osp.getmtime(path) >= min_time
