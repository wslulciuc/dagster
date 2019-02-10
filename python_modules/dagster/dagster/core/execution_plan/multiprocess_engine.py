from collections import defaultdict
import os
import multiprocessing

from dagster import check

from dagster.core.execution_context import PipelineExecutionContext
from .objects import ExecutionPlan, ExecutionStepEvent, ExecutionStepEventType, StepOutputHandle


def _execute_in_child_process(
    queue, executor, environment, execution_metadata, step_key, input_values
):
    from dagster.core.execution import ExecutionPlanSubsetInfo, iterate_execution_plan

    pipeline = executor.pipeline_fn()

    for step_event in iterate_execution_plan(
        pipeline,
        ExecutionPlanSubsetInfo.with_input_values([step_key], input_values),
        environment,
        execution_metadata.with_tags({'process_id': str(os.getpid())}),
        throw_on_user_error=False,
    ):
        data_to_put = {
            'event_type': step_event.event_type,
            'success_data': step_event.success_data,
            'failure_data': step_event.failure_data,
            'step_key': step_event.step.key,
        }

        queue.put(data_to_put)

    queue.put('DONE')
    queue.close()


def multiprocess_iterate_steps(pipeline_context, executor, step, prev_level_step_events):
    queue = multiprocessing.Queue()
    inputs_to_inject = defaultdict(dict)

    for step_input in step.step_inputs:
        inputs_to_inject[step.key][step_input.name] = prev_level_step_events[
            step_input.prev_output_handle
        ].success_data.value

    process = multiprocessing.Process(
        target=_execute_in_child_process,
        args=(
            queue,
            executor,
            pipeline_context.environment_config,
            pipeline_context.execution_metadata,
            step.key,
            inputs_to_inject,
        ),
    )
    process.start()
    while process.is_alive():
        result = queue.get()
        if result == 'DONE':
            break
        event_type = result['event_type']
        if step.key != result['step_key']:
            continue
        check.invariant(step.key == result['step_key'])
        if event_type == ExecutionStepEventType.STEP_OUTPUT:
            yield ExecutionStepEvent.step_output_event(step, result['success_data'])
        elif event_type == ExecutionStepEventType.STEP_FAILURE:
            yield ExecutionStepEvent.step_failure_event(step, result['failure_data'])
        else:
            check.failed('Not support event_type {}'.format(result.event_type))

    process.join()

    # for step_event in iterate_step_events_for_step(step, step_context, input_values):
    #     yield step_event


def _all_inputs_covered(step, results):
    for step_input in step.step_inputs:
        if step_input.prev_output_handle not in results:
            return False
    return True


class DagsterMultiprocessExecutor:
    def __init__(self, pipeline_fn):
        self.pipeline_fn = pipeline_fn

    def executor_fn(self, pipeline_context, execute_plan):
        for step_event in execute_plan_in_engine(self, pipeline_context, execute_plan):
            yield step_event


def execute_plan_in_engine(executor, pipeline_context, execution_plan):
    check.inst_param(pipeline_context, 'pipeline_context', PipelineExecutionContext)
    check.inst_param(execution_plan, 'execution_plan', ExecutionPlan)

    prev_level_step_events = {}
    for steps_at_level in execution_plan.topological_step_levels():

        for step in steps_at_level:

            step_context = pipeline_context.for_step(step)

            if not _all_inputs_covered(step, prev_level_step_events):
                result_keys = set(prev_level_step_events.keys())
                expected_outputs = [ni.prev_output_handle for ni in step.step_inputs]

                step_context.log.debug(
                    (
                        'Not all inputs covered for {step}. Not executing. Keys in result: '
                        '{result_keys}. Outputs need for inputs {expected_outputs}'
                    ).format(
                        expected_outputs=expected_outputs, step=step.key, result_keys=result_keys
                    )
                )
                continue

            input_values = {}
            for step_input in step.step_inputs:
                prev_output_handle = step_input.prev_output_handle
                input_value = prev_level_step_events[prev_output_handle].success_data.value
                input_values[step_input.name] = input_value

            for step_event in multiprocess_iterate_steps(
                pipeline_context, executor, step, prev_level_step_events
            ):
                check.inst(step_event, ExecutionStepEvent)

                yield step_event

                if step_event.event_type == ExecutionStepEventType.STEP_OUTPUT:
                    output_handle = StepOutputHandle(step, step_event.success_data.output_name)
                    prev_level_step_events[output_handle] = step_event

    # smash to allow garbage collection
    prev_level_step_events = {}
