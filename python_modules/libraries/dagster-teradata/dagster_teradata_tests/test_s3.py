import pytest
from dagster import job, op, Definitions
from dagster_teradata import teradata_resource
from dagster_aws.s3 import S3Resource


@pytest.mark.integration
def test_s3_to_teradata(tmp_path):
    @op(required_resource_keys={'teradata', 's3'})
    def example_s3_op(context):
        context.resources.teradata.s3_to_teradata(context.resources.s3,
                                                  '/s3/mt255026-test.s3.amazonaws.com/people.csv', 'people')

    @job(resource_defs={'teradata': teradata_resource, 's3': S3Resource})
    def example_job():
        example_s3_op()

    example_job.execute_in_process(
        run_config={
            'resources': {
                's3': {
                    'config': {
                        'aws_access_key_id': 'id',
                        'aws_secret_access_key': 'key',
                        'aws_session_token': 'token',
                    }
                },
                'teradata': {
                    'config': {
                        'host' : 'dbs host',
                        'user' : 'user',
                        'password' : 'password',
                        'database' : 'database',
                    }
                }
            }
        }
    )