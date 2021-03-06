from dagster import check

from dagster.core.definitions import Solid, OutputDefinition
from dagster.core.execution_context import PipelineExecutionContext

from dagster.core.types.runtime import RuntimeType

from .objects import (
    ExecutionStep,
    ExecutionValueSubplan,
    StepInput,
    StepOutput,
    StepOutputValue,
    StepKind,
)

from .utility import create_joining_subplan

MATERIALIZATION_THUNK_INPUT = 'materialization_thunk_input'
MATERIALIZATION_THUNK_OUTPUT = 'materialization_thunk_output'


def _create_materialization_lambda(runtime_type, config_spec):
    check.inst_param(runtime_type, 'runtime_type', RuntimeType)
    check.invariant(runtime_type.output_schema, 'Must have output schema')

    def _fn(_step_context, inputs):
        runtime_value = inputs[MATERIALIZATION_THUNK_INPUT]
        runtime_type.output_schema.materialize_runtime_value(config_spec, runtime_value)
        yield StepOutputValue(output_name=MATERIALIZATION_THUNK_OUTPUT, value=runtime_value)

    return _fn


def configs_for_output(solid, solid_config, output_def):
    for output_spec in solid_config.outputs:
        check.invariant(len(output_spec) == 1)
        output_name, output_spec = list(output_spec.items())[0]
        check.invariant(solid.has_output(output_name))
        if output_name == output_def.name:
            yield output_spec


def decorate_with_output_materializations(pipeline_context, solid, output_def, subplan):
    check.inst_param(pipeline_context, 'pipeline_context', PipelineExecutionContext)
    check.inst_param(solid, 'solid', Solid)
    check.inst_param(output_def, 'output_def', OutputDefinition)
    check.inst_param(subplan, 'subplan', ExecutionValueSubplan)

    solid_config = pipeline_context.environment_config.solids.get(solid.name)

    if not (solid_config and solid_config.outputs):
        return subplan

    new_steps = []

    for mat_count, output_spec in enumerate(configs_for_output(solid, solid_config, output_def)):
        new_steps.append(
            ExecutionStep(
                pipeline_context=pipeline_context,
                key='{solid}.materialization.output.{output}.{mat_count}'.format(
                    solid=solid.name, output=output_def.name, mat_count=mat_count
                ),
                step_inputs=[
                    StepInput(
                        name=MATERIALIZATION_THUNK_INPUT,
                        runtime_type=output_def.runtime_type,
                        prev_output_handle=subplan.terminal_step_output_handle,
                    )
                ],
                step_outputs=[
                    StepOutput(
                        name=MATERIALIZATION_THUNK_OUTPUT, runtime_type=output_def.runtime_type
                    )
                ],
                kind=StepKind.MATERIALIZATION_THUNK,
                solid=solid,
                compute_fn=_create_materialization_lambda(output_def.runtime_type, output_spec),
                # tags=plan_builder.get_tags(),
            )
        )

    return create_joining_subplan(
        pipeline_context,
        solid,
        '{solid}.materialization.output.{output}.join'.format(
            solid=solid.name, output=output_def.name
        ),
        new_steps,
        MATERIALIZATION_THUNK_OUTPUT,
    )
