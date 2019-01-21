import abc

from collections import namedtuple

LambdaInvocationPayload = namedtuple(
    'LambdaInvocationPayload',
    'run_id step_idx key s3_bucket s3_key_inputs s3_key_body ' 's3_key_resources s3_key_outputs',
)


class DagmaEngine(abc.ABC):
    '''Abstract base class for Dagma engines.

    Do not instantiate directly.
    '''

    @abc.abstractmethod
    def __init__(self, config):
        '''Constructor for Dagma engines.

        Parameters:
          config (DagmaEngineConfig): A strongly-typed config value.
        '''
        pass

    def deploy_runtime(self):
        '''Construct (if needed) and deploy a runtime for a Dagma engine.

        For engines that do not require a runtime, this should be a no-op.
        '''
        pass

    def deploy_pipeline(self, pipeline):
        '''Deploy a pipeline for a Dagma engine.

        For engines that do not require a deploy step, this should be a no-op.

        Parameters:
          pipeline (PipelineDefinition): The dagster pipeline to deploy.
        '''

    def execute_step_async(self, run_id):
        '''Asynchronously execute an execution_step for a Dagma engine.

        FIXME: Need to define this API
        '''
        raise NotImplementedError()

    @abc.abstractmethod
    def execute_step_sync(self, run_id):
        '''Synchronously execute an execution step for a Dagma engine.
        '''


class DagmaEngineConfig(abc.ABC):
    '''Abstract base class for dagma engine configs.
    
    Do not instantiate directly.
    '''

    pass
