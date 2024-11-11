from textwrap import dedent
from dagster import (
    Nothing,
    _check as check,
    op,
)
from dagster._core.definitions.input import In
from dagster._core.storage.tags import COMPUTE_KIND_TAG


def s3_to_teradata(dagster_decorator, decorator_name, sql, parameters=None):
    self.s3_source_key = s3_source_key
    self.public_bucket = public_bucket
    self.teradata_table = teradata_table
    self.aws_conn_id = aws_conn_id
    self.teradata_conn_id = teradata_conn_id
    self.teradata_authorization_name = teradata_authorization_name

    s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
    teradata_hook = TeradataHook(teradata_conn_id=self.teradata_conn_id)
    credentials_part = "ACCESS_ID= '' ACCESS_KEY= ''"
    if not self.public_bucket:
        # Accessing data directly from the S3 bucket and creating permanent table inside the database
        if self.teradata_authorization_name:
            credentials_part = f"AUTHORIZATION={self.teradata_authorization_name}"
        else:
            credentials = s3_hook.get_credentials()
            access_key = credentials.access_key
            access_secret = credentials.secret_key
            credentials_part = f"ACCESS_ID= '{access_key}' ACCESS_KEY= '{access_secret}'"
            token = credentials.token
            if token:
                credentials_part = credentials_part + f" SESSION_TOKEN = '{token}'"
    sql = dedent(f"""
              CREATE MULTISET TABLE {self.teradata_table} AS
              (
                    SELECT * FROM (
                        LOCATION = '{self.s3_source_key}'
                        {credentials_part}
                    ) AS d
                ) WITH DATA
                """).rstrip()

