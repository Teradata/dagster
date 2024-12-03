import os

import boto3
import pytest
from dagster import job, op, Definitions
from dagster_teradata import teradata_resource
from dagster_aws.s3 import s3_resource


@pytest.mark.integration
def test_s3_to_teradata(tmp_path):
    @op(required_resource_keys={'teradata'})
    def example_test_s3_to_teradata(context):
        session = boto3.Session()
        context.resources.teradata.s3_to_teradata(session,
                                                  '/s3/mt255026-test.s3.amazonaws.com/people.csv', 'people')

    @job(resource_defs={'teradata': teradata_resource})
    def example_job():
        example_test_s3_to_teradata()

    example_job.execute_in_process(
        run_config={
            'resources': {
                'teradata': {
                    'config': {
                        'host' : os.getenv('TERADATA_HOST'),
                        'user' : os.getenv('TERADATA_USER'),
                        'password' : os.getenv('TERADATA_PASSWORD'),
                        'database' : os.getenv('TERADATA_DATABASE'),
                    }
                }
            }
        }
    )