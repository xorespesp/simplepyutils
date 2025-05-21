import functools
import hashlib
import inspect
import json
import os
import os.path as osp
import pickle

from simplepyutils import file_io
from simplepyutils.argparse import logger

_default_cache_root = os.environ.get('CACHE_DIR')


def picklecache(path, forced=False, min_time=None):
    """Caches and restores the results of a function call on disk.
    Specifically, it returns a function decorator that makes a function cache its result in a file.
    It only evaluates the function once, to generate the cached file. The decorator also adds a
    new keyword argument to the function, called 'forced_cache_update' that can explicitly force
    regeneration of the cached file.

    It has rudimentary handling of arguments by hashing their json representation and appending it
    the hash to the cache filename. This somewhat limited, but is enough for the current uses.

    Set `min_time` to the last significant change to the code within the function.
    If the cached file is older than this `min_time`, the file is regenerated.

    Args:
        path: The path where the function's result is stored.
        forced: do not load from disk, always recreate the cached version
        min_time: recreate cached file if its modification timestamp (mtime) is older than this
           param. The format is like 2025-12-27T10:12:32 (%Y-%m-%dT%H:%M:%S)

    Returns:
        The decorator.


    Examples:
        .. code:: python

            @picklecache('/some/path/to/a/file', min_time='2025-12-27T10:12:32')
            def some_function(some_arg):
               ...
               return stuff
    """

    def decorator(f):
        @functools.wraps(f)
        def wrapped(*args, **kwargs):
            inner_forced = forced or kwargs.pop('forced_cache_update', False)
            if _default_cache_root is not None and not osp.isabs(path):
                inner_path = osp.join(_default_cache_root, path)
                hash_parent = _default_cache_root
            else:
                inner_path = path
                hash_parent = osp.dirname(path)

            bound_args = inspect.signature(f).bind(*args, **kwargs)
            args_json = json.dumps((bound_args.args, bound_args.kwargs), sort_keys=True)
            hash_string = hashlib.sha1(str(args_json).encode('utf-8')).hexdigest()[:12]

            if args or kwargs:
                noext, ext = osp.splitext(inner_path)
                suffixed_path = f'{noext}_{hash_string}{ext}'
            else:
                suffixed_path = inner_path

            if not inner_forced and file_io.is_file_newer(suffixed_path, min_time):
                logger.info(f'Loading cached data from {suffixed_path}')
                try:
                    return file_io.load_pickle(suffixed_path)
                except Exception as e:
                    error_message = str(e)
                    logger.warning(f'Could not load from {suffixed_path}, due to: {error_message}')

            if osp.exists(suffixed_path):
                logger.info(f'Recomputing data for {suffixed_path}')
            else:
                logger.info(f'Computing data for {suffixed_path}')

            result = f(*args, **kwargs)
            file_io.dump_pickle(result, suffixed_path, protocol=pickle.HIGHEST_PROTOCOL)

            if args or kwargs:
                hash_path = osp.join(hash_parent, 'hashes', hash_string)
                file_io.write_file(args_json, hash_path)

            return result

        return wrapped

    return decorator


def set_default_cache_root(cache_root):
    global _default_cache_root
    _default_cache_root = cache_root
