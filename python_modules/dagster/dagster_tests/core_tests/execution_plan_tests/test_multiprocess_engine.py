from dagster import (
    DependencyDefinition,
    InputDefinition,
    PipelineDefinition,
    execute_pipeline,
    lambda_solid,
)
from dagster.core.execution import DagsterSimpleExecutor
from dagster.core.execution_plan.multiprocess_engine import DagsterMultiprocessExecutor


def test_diamond_simple_execution():
    result = execute_pipeline(define_diamond_pipeline(), engine_executor=DagsterSimpleExecutor())
    assert result.success
    assert result.result_for_solid('adder').transformed_value() == 11


def test_diamond_multi_execution():
    result = execute_pipeline(
        define_diamond_pipeline(),
        engine_executor=DagsterMultiprocessExecutor(define_diamond_pipeline),
    )
    assert result.success
    assert result.result_for_solid('adder').transformed_value() == 11


def define_diamond_pipeline():
    @lambda_solid
    def return_two():
        return 2

    @lambda_solid(inputs=[InputDefinition('num')])
    def add_three(num):
        return num + 3

    @lambda_solid(inputs=[InputDefinition('num')])
    def mult_three(num):
        return num * 3

    @lambda_solid(inputs=[InputDefinition('left'), InputDefinition('right')])
    def adder(left, right):
        return left + right

    return PipelineDefinition(
        name='diamond_execution',
        solids=[return_two, add_three, mult_three, adder],
        dependencies={
            'add_three': {'num': DependencyDefinition('return_two')},
            'mult_three': {'num': DependencyDefinition('return_two')},
            'adder': {
                'left': DependencyDefinition('add_three'),
                'right': DependencyDefinition('mult_three'),
            },
        },
    )
