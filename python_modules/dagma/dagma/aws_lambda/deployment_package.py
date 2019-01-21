"""Utilities to construct and upload the deployment package used by our Lambda handler."""

import contextlib

# https://github.com/PyCQA/pylint/issues/73
import distutils.spawn  # pylint: disable=no-name-in-module, import-error
import os
import subprocess

from botocore.exceptions import ClientError

from dagster import check
from dagster.utils.zip import zip_folder

from ..utils import tempdirs
from ..version import __version__
from .config import PYTHON_DEPENDENCIES


def _which(exe):
    # https://github.com/PyCQA/pylint/issues/73
    return distutils.spawn.find_executable(exe)  # pylint: disable=no-member


def _get_deployment_package_key():
    return 'dagma_runtime_{version}'.format(version=__version__)


@contextlib.contextmanager
def _construct_deployment_package(context, key, python_dependencies=None):

    python_dependencies = check.opt_list_param(
        python_dependencies, 'python_dependencies', of_type=str
    )

    if _which('pip') is None:
        raise Exception('Couldn\'t find \'pip\' -- can\'t construct a deployment package')

    if _which('git') is None:
        raise Exception('Couldn\'t find \'git\' -- can\'t construct a deployment package')

    with tempdirs(2) as (deployment_package_dir, archive_dir):
        for python_dependency in PYTHON_DEPENDENCIES + python_dependencies:
            process = subprocess.Popen(
                ['pip', 'install', python_dependency, '--target', deployment_package_dir],
                stderr=subprocess.PIPE,
                stdout=subprocess.PIPE,
            )
            for line in iter(process.stdout.readline, b''):
                context.debug(line.decode('utf-8'))

        archive_path = os.path.join(archive_dir, key)

        try:
            pwd = os.getcwd()
            os.chdir(deployment_package_dir)
            zip_folder('.', archive_path)
            context.debug(
                'Zipped archive at {archive_path}: {size} bytes'.format(
                    archive_path=archive_path, size=os.path.getsize(archive_path)
                )
            )
        finally:
            os.chdir(pwd)

        yield archive_path
