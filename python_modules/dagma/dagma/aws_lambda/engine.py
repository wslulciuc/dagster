"""The AWS Lambda execution engine."""

import base64
import json
import os

from collections import namedtuple

from dagster import check
from dagster.core.execution_context import RuntimeExecutionContext
from dagster.core.execution_plan.objects import ExecutionPlan

from ..types import DagmaEngine, LambdaInvocationPayload
from .config import ASSUME_ROLE_POLICY_DOCUMENT, BUCKET_POLICY_DOCUMENT_TEMPLATE, LAMBDA_MEMORY_SIZE
from .utils import get_function_name, get_input_key, get_resources_key, get_step_key


from ..types import DagmaEngine, DagmaEngineConfig

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))


def _get_python_runtime():
    # if sys.version_info[0] < 3:
    #     return 'python2.7'
    return 'python3.6'


class LambdaEngineConfig(
    namedtuple(
        '_LambdaEngineConfig',
        'aws_region sessionmaker runtime_s3_bucket execution_s3_bucket storage',
    ),
    DagmaEngineConfig,
):
    """Typed config for the lambda execution engine"""


class LambdaEngine(DagmaEngine):
    def __init__(self, engine_config):
        check.inst_param(engine_config, 'engine_config', LambdaEngineConfig)

        self.engine_config = engine_config

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

    def _get_or_create_iam_role(self):
        try:
            # TODO make the role name configurable
            role = self.iam_client.create_role(
                RoleName='dagster_lambda_iam_role',
                AssumeRolePolicyDocument=ASSUME_ROLE_POLICY_DOCUMENT,
            )
        except self.iam_client.exceptions.EntityAlreadyExistsException:
            role = self.iam_resource.Role('dagster_lambda_iam_role')
            role.load()
        return role

    def _get_or_create_s3_bucket(self, bucket, role, context):
        try:
            self.s3_client.create_bucket(
                ACL='private',
                Bucket=bucket,
                CreateBucketConfiguration={'LocationConstraint': self.aws_region},
            )
        except self.s3_client.exceptions.BucketAlreadyOwnedByYou:
            pass

        policy = BUCKET_POLICY_DOCUMENT_TEMPLATE.format(
            role_arn=role.arn, bucket_arn='arn:aws:s3:::' + bucket
        )
        context.debug(policy)
        self.s3_client.put_bucket_policy(Bucket=bucket, Policy=policy)

    def _seed_intermediate_results(self, context):
        intermediate_results = {}
        return self.storage.put_object(
            key=get_input_key(context, 0), body=serialize(intermediate_results)
        )

    def _upload_step(self, step_idx, step, context):
        return self.storage.put_object(key=get_step_key(context, step_idx), body=serialize(step))

    def _create_lambda_step(self, deployment_package, context, role):
        runtime = _get_python_runtime()
        context.debug(
            'About to create function with bucket {bucket} deployment_package_key {deploy_key}'.format(
                bucket=context.resources.dagma.s3_bucket, deploy_key=deployment_package
            )
        )
        res = self.lambda_client.create_function(
            FunctionName=get_function_name(context),
            Runtime=runtime,
            Role=role.arn,
            Handler='dagma.aws_lambda_handler',
            Code={'S3Bucket': context.resources.dagma.runtime_bucket, 'S3Key': deployment_package},
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

    def _upload_deployment_package(context, key, path):
        context.debug('Uploading deployment package')
        with open(path, 'rb') as fd:
            return context.resources.dagma.storage.client.put_object(
                Bucket=context.resources.dagma.runtime_bucket, Key=key, Body=fd
            )
        context.debug('Done uploading deployment package')

    def _create_and_upload_deployment_package(context, key):
        with _construct_deployment_package(context, key) as deployment_package_path:
            _upload_deployment_package(context, key, deployment_package_path)

    def _get_deployment_package(context, key):
        try:
            context.resources.dagma.storage.client.head_object(
                Bucket=context.resources.dagma.runtime_bucket, Key=key
            )
            return key
        except ClientError:
            return None

    def get_or_create_deployment_package(context):
        deployment_package_key = _get_deployment_package_key()

        context.debug(
            'Looking for deployment package at {s3_bucket}/{s3_key}'.format(
                s3_bucket=context.resources.dagma.runtime_bucket, s3_key=deployment_package_key
            )
        )

        if _get_deployment_package(context, deployment_package_key):
            context.debug('Found deployment package!')
            return deployment_package_key

        context.debug('Creating deployment package...')
        _create_and_upload_deployment_package(context, deployment_package_key)

        return deployment_package_key

    def deploy_runtime(self):
        """Idempotently deploy the dagma runtime to AWS lambda."""
        return super().deploy_runtime()

    def deploy_pipeline(self, pipeline):
        self.deploy_runtime()

        return super().deploy_pipeline(pipeline)

    def execute_step_async(lambda_client, lambda_step, context, payload):
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

    def _execute_step_sync(lambda_client, lambda_step, context, payload):
        res = lambda_client.invoke(
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
        check.inst_param(context, 'context', RuntimeExecutionContext)
        check.inst_param(execution_plan, 'execution_plan', ExecutionPlan)

        steps = list(execution_plan.topological_steps())

        if not steps:
            context.debug(
                'Pipeline {pipeline} has no nodes and no execution will happen'.format(
                    pipeline=context.pipeline
                )
            )
            return

        context.debug(
            'About to compile the compute node graph to lambda in the following order {order}'.format(
                order=[step.key for step in steps]
            )
        )

        check.invariant(len(steps[0].step_inputs) == 0)

        aws_lambda_client = context.resources.dagma.session.client('lambda')
        aws_iam_client = context.resources.dagma.session.client('iam')
        aws_iam_resource = context.resources.dagma.session.resource('iam')
        aws_cloudwatch_client = context.resources.dagma.session.client('logs')
        aws_s3_client = context.resources.dagma.session.client('s3')
        aws_region_name = context.resources.dagma.aws_region_name

        context.debug('Creating IAM role')
        role = _get_or_create_iam_role(aws_iam_client, aws_iam_resource)

        context.debug('Creating S3 bucket')
        _get_or_create_s3_bucket(aws_s3_client, aws_region_name, role, context)

        context.debug('Seeding intermediate results')
        _seed_intermediate_results(context)

        deployment_package_key = get_or_create_deployment_package(context)

        context.debug('Uploading execution_context')
        context.resources.dagma.storage.put_object(
            key=get_resources_key(context), body=serialize(context.resources)
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

            final_results = deserialize(final_results_object['Body'].read())

            return final_results

        finally:
            if cleanup_lambda_functions:
                context.debug(
                    'Deleting lambda function: {name}'.format(name=lambda_step['FunctionName'])
                )
                aws_lambda_client.delete_function(FunctionName=lambda_step['FunctionName'])
