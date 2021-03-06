import sqlalchemy

from dagster import check

from dagster.utils.test import create_test_pipeline_execution_context

import dagster_sqlalchemy as dagster_sa

from dagster_sqlalchemy.common import (
    DefaultSqlAlchemyResources,
    SqlAlchemyResource,
    check_supports_sql_alchemy_resource,
)


def create_sql_alchemy_context_from_sa_resource(sa_resource, *args, **kwargs):
    check.inst_param(sa_resource, 'sa_resource', SqlAlchemyResource)
    resources = DefaultSqlAlchemyResources(sa_resource)
    context = create_test_pipeline_execution_context(resources=resources, *args, **kwargs)
    return check_supports_sql_alchemy_resource(context)


# disable warnings about malformed inheritance
# pylint: disable=W0221, W0223
class MockEngine(sqlalchemy.engine.Engine):
    def __init__(self):
        super(MockEngine, self).__init__(None, None, None)

    def connect(self):
        raise Exception('should not call')

    def raw_connection(self):
        raise Exception('should not call')


def test_mock():
    sa_resource = dagster_sa.SqlAlchemyResource(engine=MockEngine(), mock_sql=True)
    legacy_context = create_sql_alchemy_context_from_sa_resource(sa_resource)
    dagster_sa.common.execute_sql_text_on_sa_resource(legacy_context.resources.sa, 'NOPE')
