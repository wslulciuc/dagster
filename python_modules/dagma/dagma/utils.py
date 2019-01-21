import contextlib
import errno
import os
import shutil
import tempfile

from collections import namedtuple


def mkdir_p(path):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise


@contextlib.contextmanager
def tempdirs(i=1):
    try:
        dirs = []
        for _ in range(i):
            dirs.append(tempfile.mkdtemp())
        if not dirs:
            yield None
        if len(dirs) == 1:
            yield dirs[0]
        yield tuple(dirs)
    finally:
        for dir_ in dirs:
            try:
                shutil.rmtree(dir_)
            except IOError as exc:
                continue


def format_str_options(options):
    return '|'.join(['`\'{option}\'`'.format(option=option) for option in options])
