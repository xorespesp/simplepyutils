"""Simple Python utilities for common tasks."""

from simplepyutils.argparse import FLAGS, initialize, logger, flags_getter
from simplepyutils.file_io import (
    dump_json,
    dump_pickle,
    ensure_parent_dir_exists,
    is_file_newer,
    is_pickle_readable,
    load_json,
    load_pickle,
    load_yaml,
    read_file,
    read_lines,
    write_file,
)
from simplepyutils.itertools import (
    nested_spy_first,
    prefetch,
    roundrobin,
    repeat_n,
    filter_by_index,
    SlicableForwardSlice,
)
from simplepyutils.misc import (
    all_disjoint,
    groupby,
    groupby_map,
    is_running_in_jupyter_notebook,
    itemsetter,
    parallel_map_with_progbar,
    progressbar,
    progressbar_items,
    rounded_int_tuple,
    sorted_recursive_glob,
    zip_progressbar,
    terminate_on_parent_death,
)
from simplepyutils.picklecachefun import picklecache
from simplepyutils.strings import (
    last_path_components,
    natural_sort_key,
    natural_sort_key_float,
    natural_sorted,
    path_range,
    path_stem,
    replace_extension,
    split_path,
    str_range,
)
from simplepyutils.throttledpool import ThrottledPool
from simplepyutils import argparse
