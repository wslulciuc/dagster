import runpy
import sys

import pytest


def test_entrypoint():
    old_argv = sys.argv
    sys.argv = ['']
    try:
        with pytest.raises(SystemExit) as exc:
            runpy.run_module('dagma', run_name='__main__', alter_sys=True)
        assert exc.value.code == 0
    finally:
        sys.argv = old_argv
