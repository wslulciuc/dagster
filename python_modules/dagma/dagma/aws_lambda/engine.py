"""The AWS Lambda execution engine."""

import contextlib
import base64
import json
import os
import pickle
import subprocess

import botocore

from collections import namedtuple

from dagster import check
from dagster.core.execution_context import RuntimeExecutionContext
from dagster.core.execution_plan.objects import ExecutionPlan
from dagster.utils import script_relative_path
from dagster.utils.zip import zip_folder

from ..types import DagmaEngine, DagmaEngineConfig, LambdaInvocationPayload
from ..utils import mkdir_p, tempdirs, which
from .config import (
    ASSUME_ROLE_POLICY_DOCUMENT,
    BUCKET_POLICY_DOCUMENT_TEMPLATE,
    LAMBDA_MEMORY_SIZE,
    PYTHON_DEPENDENCIES,
)


SOURCE_DIR = script_relative_path('.')


# TODO: make this configurable
def _get_python_runtime():
    # if sys.version_info[0] < 3:
    #     return 'python2.7'
    return 'python3.6'


class LambdaEngineConfig(
    namedtuple(
        '_LambdaEngineConfig',
        'aws_region sessionmaker runtime_s3_bucket execution_s3_bucket storage '
        'python_dependencies includes',
    ),
    DagmaEngineConfig,
):
    """Typed config for the lambda execution engine"""


class LambdaEngine(DagmaEngine):
    def __init__(self, engine_config, requirements, includes, root_directory):
        check.inst_param(engine_config, 'engine_config', LambdaEngineConfig)

        self.engine_config = engine_config
        self.requirements = requirements
        self.includes = includes
        self.root_directory = root_directory

        super(DagmaEngine, self).__init__()

    @property
    def session(self):
        """The boto3 session."""
        return self.engine_config.sessionmaker()

    @property
    def lambda_client(self):
        """The boto3 lambda client."""
        return self.session.client('lambda')

    @property
    def iam_client(self):
        """The boto3 iam client."""
        return self.session.client('iam')

    @property
    def iam_resource(self):
        """The boto3 iam resource."""
        return self.session.resource('iam')

    @property
    def cloudwatch_client(self):
        """The boto3 cloudwatch client."""
        return self.session.client('logs')

    @property
    def s3_client(self):
        """The boto3 cloudwatch client."""
        return self.session.client('s3')

    @property
    def aws_region(self):
        """The aws region."""
        return self.engine_config.aws_region

    @property
    def storage(self):
        """The storage manager."""
        return self.engine_config.storage

    @property
    def runtime_bucket(self):
        """The S3 bucket in which to store the runtime."""
        return self.engine_config.runtime_s3_bucket

    @property
    def execution_bucket(self):
        """The S3 bucket in which to store the results of the computation."""
        return self.engine_config.execution_s3_bucket

    def function_key(self, context):
        return '{run_id}_function'.format(run_id=context.run_id)

    def step_key(self, context, step_idx):
        return '{run_id}_step_{step_idx}.pickle'.format(run_id=context.run_id, step_idx=step_idx)

    def intermediate_results_key(self, context, step_idx):
        return '{run_id}_intermediate_results_{step_idx}.pickle'.format(
            run_id=context.run_id, step_idx=step_idx
        )

    def deployment_package_key(self, context):
        return '{run_id}_deployment_package.zip'.format(run_id=context.run_id)

    def resources_key(self, context):
        return '{run_id}_resources.pickle'.format(run_id=context.run_id)

    def _get_or_create_iam_role(self, context):
        try:
            context.debug('Retrieving or creating lambda IAM role...')
            # TODO make the role name configurable
            role = self.iam_client.create_role(
                RoleName='dagster_lambda_iam_role',
                AssumeRolePolicyDocument=ASSUME_ROLE_POLICY_DOCUMENT,
            )
        except botocore.exceptions.ClientError as e:
            if 'EntityAlreadyExistsException' == e.__class__.__name__:
                context.debug('Found existing IAM role!')
                role = self.iam_resource.Role('dagster_lambda_iam_role')
                role.load()
            else:
                raise
        else:
            context.debug('Created new IAM role!')
        return role

    def _get_or_create_s3_bucket(self, context, bucket, role):
        try:
            self.s3_client.create_bucket(
                ACL='private',
                Bucket=bucket,
                CreateBucketConfiguration={'LocationConstraint': self.aws_region},
            )
        except botocore.exceptions.ClientError as e:
            if 'BucketAlreadyOwnedByYou' == e.__class__.__name__:
                pass
            else:
                raise

        policy = BUCKET_POLICY_DOCUMENT_TEMPLATE.format(
            role_arn=role.arn, bucket_arn='arn:aws:s3:::' + bucket
        )
        context.debug(policy)
        self.s3_client.put_bucket_policy(Bucket=bucket, Policy=policy)

    def _seed_intermediate_results(self, context):
        intermediate_results = {}
        return self.storage.put_object(
            key=self.intermediate_results_key(context, 0), body=pickle.dumps(intermediate_results)
        )

    def _upload_step(self, step_idx, step, context):
        return self.storage.put_object(
            key=self.step_key(context, step_idx), body=pickle.dumps(step)
        )

    def _create_lambda_step(self, deployment_package, context, role):
        runtime = _get_python_runtime()
        context.debug(
            'About to create function with bucket {bucket} deployment_package_key '
            '{deploy_key}'.format(bucket=self.execution_bucket, deploy_key=deployment_package)
        )
        res = self.lambda_client.create_function(
            FunctionName=self.function_key(context),
            Runtime=runtime,
            Role=role.arn,
            Handler='dagma.aws_lambda_handler',
            Code={'S3Bucket': self.execution_bucket, 'S3Key': deployment_package},
            Description='Handler for run {run_id} step {step_idx}'.format(
                run_id=context.run_id, step_idx='0'
            ),
            Timeout=900,
            MemorySize=LAMBDA_MEMORY_SIZE,
            Publish=True,
            Tags={'dagster_lambda_run_id': context.run_id},
        )
        context.debug(str(res))
        return res

    def _upload_deployment_package(self, context, key, path):
        context.debug('Uploading deployment package')
        with open(path, 'rb') as fd:
            return self.storage.client.put_object(Bucket=self.execution_bucket, Key=key, Body=fd)
        context.debug('Done uploading deployment package')

    def _create_deployment_package(self, context):

        deployment_package_key = self.deployment_package_key(context)

        with self._construct_deployment_package(
            context, deployment_package_key
        ) as deployment_package_path:
            self._upload_deployment_package(
                context, deployment_package_key, deployment_package_path
            )

        return deployment_package_key

    @contextlib.contextmanager
    def _construct_deployment_package(self, context, key):

        if which('pip') is None:
            raise Exception('Couldn\'t find \'pip\' -- can\'t construct a deployment package')

        if which('git') is None:
            raise Exception('Couldn\'t find \'git\' -- can\'t construct a deployment package')

        with tempdirs(2) as (deployment_package_dir, archive_dir):
            for python_dependency in PYTHON_DEPENDENCIES + self.requirements:
                process = subprocess.Popen(
                    ['pip', 'install', python_dependency, '--target', deployment_package_dir],
                    stderr=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                )
                for line in iter(process.stdout.readline, b''):
                    context.debug(line.decode('utf-8'))

            archive_path = os.path.join(archive_dir, key)

            for include in self.includes:
                with open(include, 'r') as from_fd:
                    relative_path = os.path.relpath(include, self.root_directory)
                    dir_path = os.path.join(archive_path, os.path.dirname(include))
                    mkdir_p(dir_path)
                    with open(os.path.join(archive_path, relative_path), 'w') as to_fd:
                        to_fd.write(from_fd.read())

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

    def deploy_pipeline(self, context, pipeline):
        """Idempotently deploy the dagma pipeline to AWS lambda."""
        role = self._get_or_create_iam_role(context)
        self._get_or_create_s3_bucket(context, self.runtime_bucket, role)
        self._get_or_create_s3_bucket(context, self.execution_bucket, role)
        deployment_package_key = self._create_deployment_package(context)

        context.debug('Seeding intermediate results')
        self._seed_intermediate_results(context)

        context.debug('Uploading resources')
        self.storage.put_object(
            key=self.resources_key(context), body=pickle.dumps(context.resources)
        )

        for step_idx, step in enumerate(steps):
            context.debug(
                'Uploading step {step_key}: {s3_key}'.format(
                    step_key=step.key, s3_key=get_step_key(context, step_idx)
                )
            )
            _upload_step(aws_s3_client, step_idx, step, context)

        try:
            lambda_step = _create_lambda_step(
                aws_lambda_client, deployment_package_key, context, role
            )


    def execute_step_async(self, lambda_step, context, payload):
        raise NotImplementedError()
        #   InvocationType='Event'|'RequestResponse'|'DryRun',
        # when we switch to Event, we'll need to poll Cloudwatch
        # log_group_name = '/aws/lambda/{function_name}'
        # .format(function_name='{run_id}_hello_world'.format(run_id=run_id))
        # aws_cloudwatch_client.get_log_events(
        # logGroupName='/aws/lambda/{function_name}'
        # .format(function_name='{run_id}_hello_world'.format(run_id=run_id)))
        # aws_cloudwatch_client.describe_log_streams(
        #     logGroupName=log_group_name,
        #     orderBy='LastEventTime',
        #     descending=True,
        # #     nextToken='string',
        # #     limit=123
        # )

    def execute_step_sync(self, lambda_step, context, payload):
        res = self.lambda_client.invoke(
            FunctionName=lambda_step['FunctionArn'],
            Payload=json.dumps({'config': list(payload)}),
            InvocationType='RequestResponse',
            LogType='Tail',
        )

        for line in base64.b64decode(res['LogResult']).split(b'\n'):
            if True:
                context.info(str(line))
                #  FIXME switch on the log type
            else:
                context.info(line)

    def execute_plan(self, context, execution_plan, cleanup_lambda_functions=True, local=False):
        """Core executor."""



            for step_idx, _ in enumerate(steps):
                payload = LambdaInvocationPayload(
                    context.run_id,
                    step_idx,
                    steps[step_idx].key,
                    context.resources.dagma.s3_bucket,
                    get_input_key(context, step_idx),
                    get_step_key(context, step_idx),
                    get_resources_key(context),
                    get_input_key(context, step_idx + 1),
                )

                _execute_step_sync(aws_lambda_client, lambda_step, context, payload)
                # _poll_for_completion(step_handle, context)  # TODO: Need an error handling path here

            final_results_object = context.resources.dagma.storage.get_object(
                key=get_input_key(context, step_idx + 1)
            )

            final_results = pickle.loads(final_results_object['Body'].read())

            return final_results

        finally:
            if cleanup_lambda_functions:
                context.debug(
                    'Deleting lambda function: {name}'.format(name=lambda_step['FunctionName'])
                )
                aws_lambda_client.delete_function(FunctionName=lambda_step['FunctionName'])
