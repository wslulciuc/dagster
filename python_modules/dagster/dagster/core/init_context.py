from collections import namedtuple

import warnings

from dagster import check

from .definitions.pipeline import PipelineDefinition
from .definitions.resource import ResourceDefinition


class InitContext(namedtuple('_InitContext', 'context_config pipeline_def run_id')):
    '''
    InitContext is the context object context creation functions In effect, it is 
    the state available to those functions, and any function that is called 
    prior to pipeline execution, plus the configuration value for that context.
    '''

    def __new__(cls, context_config, pipeline_def, run_id):
        return super(InitContext, cls).__new__(
            cls,
            context_config,
            check.inst_param(pipeline_def, 'pipeline_def', PipelineDefinition),
            check.str_param(run_id, 'run_id'),
        )

    @property
    def config(self):
        warnings.warn(
            (
                'As of 0.3.2 the config property is deprecated. Use context_config instead. '
                'This will be removed in 0.4.0.'
            )
        )
        return self.context_config


class InitResourceContext(
    namedtuple(
        'InitResourceContext', 'context_config resource_config pipeline_def resource_def run_id'
    )
):
    '''
    Similar to InitContext, but is resource specific. It includes all the properties
    in the InitContext, plus the resource config and the resource definition
    '''

    def __new__(cls, context_config, resource_config, pipeline_def, resource_def, run_id):
        return super(InitResourceContext, cls).__new__(
            cls,
            context_config,
            resource_config,
            check.inst_param(pipeline_def, 'pipeline_def', PipelineDefinition),
            check.inst_param(resource_def, 'resource_def', ResourceDefinition),
            check.str_param(run_id, 'run_id'),
        )

    @property
    def config(self):
        warnings.warn(
            (
                'As of 0.3.2 the config property is deprecated. Use resource_config instead.'
                'This will be removed in 0.4.0.'
            )
        )
        return self.resource_config
