import os

import pytest
from dagster import job, op, Definitions
from dagster_teradata import teradata_resource
from dagster_aws.s3 import s3_resource


@pytest.mark.integration
def test_s3_to_teradata(tmp_path):
    @op(required_resource_keys={'teradata', 's3'})
    def example_s3_op(context):
        context.resources.teradata.create_teradata_compute_cluster('ShippingCG01', 'Shipping')


    @job(resource_defs={'teradata': teradata_resource, 's3': s3_resource})
    def example_job():
        example_s3_op()

    example_job.execute_in_process(
        run_config={
            'resources': {
                'teradata': {
                    'config': {
                        'host' : os.getenv("TERADATA_HOST"),
                        'user' : os.getenv("TERADATA_USER"),
                        'password' : os.getenv("TERADATA_PASSWORD"),
                        'database' : os.getenv("TERADATA_DATABASE"),
                    }
                }
            }
        }
    )