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
                        'aws_access_key_id': 'us-west-1',
                        'aws_secret_access_key': 'us-west-1',
                        'aws_session_token': 'IQoJb3JpZ2luX2VjEOL//////////wEaCXVzLXdlc3QtMiJGMEQCIDm/ApVPw3tZZBZ43Do1gwH/QMtte4Au/CbT/h03BDGOAiAxdjJKz8PkBRpbyPKTd5jabOz4j+cNV/deGf42K+/mZiqaAwhbEAMaDDY0NzE3NDY0MzU5NCIM2gZkANULvNX6r/mAKvcC43CaUMbFmV+l56OJT4KAxu2Q4twVC/2q/YxvYAxnPSEY4oPoeZ+VRxigbtvL8pptoS09uZegFMBXIRm3CyT7BqqWv3uijECUp3pi6LKN/bfaKI1ddBdMbhertd1wV025plpuo20HLxmAIOsPXOpa/qII7djEpdSS7DOqlCmO+1SND+ag0DSUJtaSMHjVrOkwh45fxw6W8UFYTbB8+ICaq1PgVGDw/XoED7tXPBN37S6Jnte+yX90BpqntDe6bSyDYXjIZEC80/k9AwyHWdOPL+v1Ipbc6k3c83ztBIjZYbU8w/C/p2oko75dprw43J7xCXKb8gUrPLkUfNLAFzJj1R7Q7YlXYAgS25PANc+UFhBpbB5yITrm/1SiFsfvCSJO7iIQIRotUwkdsFjXJxz+ob2Vepw9l+SaUtqdBabnjdBzk2NJhCKH5qStpTEZI5f1nWUbPOPEEboOvR1ztF/A4cSWAnHTHV7GSwAsUlAmrxL2NoEME9uEMKnUgrkGOqcBMEUjaJzfyOfmoUMtsYO7f3VYSLJscH3RbhzmkpkzkAIJJzw/+K+RpwBm8fCh55QSmsB1+bdvi9I+gG+RmHJSFiT9AzbgL/O4MOKqQoRMrPoiife1dFnBfFW0ANDVt2BhQ4ragvt6Sl4Q7YuLGVLD5Zp43coUi21svwMuFcvf6EqQEW4tTGp+8EuZ5wwqWiHsqkgz2eAtB86mSrB/77v1qN6v1lbWM28=',
                    }
                },
                'teradata': {
                    'config': {
                        'host' : 'sdt47039.labs.teradata.com',
                        'user' : 'mt255026',
                        'password' : 'mt255026',
                        'database' : 'mt255026',
                    }
                }
            }
        }
    )