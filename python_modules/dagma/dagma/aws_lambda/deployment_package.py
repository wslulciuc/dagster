"""Utilities to construct and upload the deployment package used by our Lambda handler."""

import contextlib

import os
import subprocess

from dagster import check
from dagster.utils.zip import zip_folder

from ..utils import tempdirs, which
from .config import PYTHON_DEPENDENCIES


@contextlib.contextmanager
def _construct_deployment_package(context, key, python_dependencies=None, includes=None):

    python_dependencies = check.opt_list_param(
        python_dependencies, 'python_dependencies', of_type=str
    )

    includes = check.opt_list_param(includes, 'includes', of_type=str)

    if which('pip') is None:
        raise Exception('Couldn\'t find \'pip\' -- can\'t construct a deployment package')

    if which('git') is None:
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
