import base64
import sys
import warnings
from contextlib import closing, contextmanager
from datetime import datetime
from typing import Any, Dict, Iterator, List, Mapping, Optional, Sequence, Union
from textwrap import dedent
import dagster._check as check
from dagster import (
    ConfigurableResource,
    IAttachDifferentObjectToOpContext,
    get_dagster_logger,
    resource,
)
from dagster._utils.cached_method import cached_method
from dagster._annotations import public
from dagster._core.definitions.resource_definition import dagster_maintained_resource
from dagster._core.storage.event_log.sql_event_log import SqlDbConnection
from dagster_aws.s3 import S3Resource
from pydantic import Field, validator

import teradatasql


class TeradataResource(ConfigurableResource, IAttachDifferentObjectToOpContext):
    host: str = Field(default=None, description="Teradata Database Hostname")

    user: str = Field(description="User login name.")

    password: Optional[str] = Field(default=None, description="User password.")

    database: Optional[str] = Field(
        default=None,
        description=("Name of the default database to use."),
    )

    @property
    @cached_method
    def _connection_args(self) -> Mapping[str, Any]:
        conn_args = {
            k: self._resolved_config_dict.get(k)
            for k in (
                "host",
                "user",
                "password",
                "database",
            )
            if self._resolved_config_dict.get(k) is not None
        }
        return conn_args

    @classmethod
    def _is_dagster_maintained(cls) -> bool:
        return True

    @public
    @contextmanager
    def get_connection(
        self
    ) -> Iterator[Union[SqlDbConnection, teradatasql.TeradataConnection]]:
        teradata_conn = teradatasql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
        )
        yield teradata_conn


class TeradataConnection:
    """A connection to Teradata that can execute queries. In general this class should not be
    directly instantiated, but rather used as a resource in an op or asset via the
    :py:func:`teradata_resource`.

    Note that the TeradataConnection is only used by the teradata_resource. The Pythonic TeradataResource does
    not use this TeradataConnection class.
    """

    def __init__(
            self, config: Mapping[str, str], log, teradata_connection_resource: TeradataResource
    ):
        self.teradata_connection_resource = teradata_connection_resource
        self.log = log

    @public
    @contextmanager
    def get_connection(
            self, raw_conn: bool = True
    ) -> Iterator[Union[SqlDbConnection, teradatasql.TeradataConnection]]:
        """Gets a connection to Teradata as a context manager.

        If using the execute_query, execute_queries, or load_table_from_local_parquet methods,
        you do not need to create a connection using this context manager.

        Args:
            raw_conn (bool): If using the sqlalchemy connector, you can set raw_conn to True to create a raw
                connection. Defaults to True.

        Examples:
            .. code-block:: python

                @op(
                    required_resource_keys={"teradata"}
                )
                def get_query_status(query_id):
                    with context.resources.teradata.get_connection() as conn:
                        # conn is a Teradata Connection object or a SQLAlchemy Connection if
                        # sqlalchemy is specified as the connector in the Teradata Resource config

                        return conn.get_query_status(query_id)

        """
        with self.teradata_connection_resource.get_connection(raw_conn=raw_conn) as conn:
            yield conn

    @public
    def execute_query(
            self,
            sql: str,
    ):
        """Execute a query in Teradata.

        Args:
            sql (str): the query to be executed

        Examples:
            .. code-block:: python

                @op
                def drop_database(teradata: TeradataResource):
                    teradata.execute_query(
                        "DROP DATABASE IF EXISTS MY_DATABASE"
                    )
        """
        check.str_param(sql, "sql")

        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                self.log.info("Executing query: " + sql)
                cursor.execute(sql)

    @public
    def execute_queries(
            self,
            sql_queries: Sequence[str],
    ):
        """Execute multiple queries in Teradata.

        Args:
            sql_queries (str): List of queries to be executed in series

        Examples:
            .. code-block:: python

                @op
                def create_fresh_database(teradata: TeradataResource):
                    queries = ["DROP DATABASE IF EXISTS MY_DATABASE", "CREATE DATABASE MY_DATABASE"]
                    teradata.execute_queries(
                        sql_queries=queries
                    )

        """
        check.sequence_param(sql_queries, "sql_queries", of_type=str)

        results: List[Any] = []
        with self.get_connection() as conn:
            with closing(conn.cursor()) as cursor:
                for raw_sql in sql_queries:
                    sql = raw_sql.encode("utf-8") if sys.version_info[0] < 3 else raw_sql
                    self.log.info("Executing query: " + sql)
                    parameters = dict(parameters) if isinstance(parameters, Mapping) else parameters
                    cursor.execute(sql, parameters)

    @public
    def s3_to_teradata(
            self,
            s3: S3Resource,
            s3_source_key: str,
            teradata_table: str,
            public_bucket: bool = False,
            teradata_authorization_name: str = ""
    ):
        """Loads CSV, JSON and Parquet format data from Amazon S3 to Teradata.

        Args:
            :param s3_source_key: The URI format specifying the location of the S3 bucket.(templated)
                The URI format is /s3/YOUR-BUCKET.s3.amazonaws.com/YOUR-BUCKET-NAME.
                Refer to
                https://docs.teradata.com/search/documents?query=native+object+store&sort=last_update&virtual-field=title_only&content-lang=en-US
            :param public_bucket: Specifies whether the provided S3 bucket is public. If the bucket is public,
                it means that anyone can access the objects within it via a URL without requiring authentication.
                If the bucket is private and authentication is not provided, the operator will throw an exception.
            :param teradata_table: The name of the teradata table to which the data is transferred.(templated)
            :param aws_conn_id: The Airflow AWS connection used for AWS credentials.
            :param teradata_conn_id: The connection ID used to connect to Teradata
                :ref:`Teradata connection <howto/connection:Teradata>`.
            :param teradata_authorization_name: The name of Teradata Authorization Database Object,
                is used to control who can access an S3 object store.
                Refer to
                https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Setting-Up-Access/Controlling-Foreign-Table-Access-with-an-AUTHORIZATION-Object

        Examples:
            .. code-block:: python

                import pandas as pd
                import pyarrow as pa
                import pyarrow.parquet as pq

                @op
                def write_parquet_file(teradata: TeradataResource):
                    df = pd.DataFrame({"one": [1, 2, 3], "ten": [11, 12, 13]})
                    table = pa.Table.from_pandas(df)
                    pq.write_table(table, "example.parquet')
                    teradata.load_table_from_local_parquet(
                        src="example.parquet",
                        table="MY_TABLE"
                    )

        """

        credentials_part = "ACCESS_ID= '' ACCESS_KEY= ''"

        if not public_bucket:
            # Accessing data directly from the S3 bucket and creating permanent table inside the database
            if teradata_authorization_name:
                credentials_part = f"AUTHORIZATION={teradata_authorization_name}"
            else:
                access_key = s3.aws_access_key_id
                access_secret = s3.aws_secret_access_key
                credentials_part = f"ACCESS_ID= '{access_key}' ACCESS_KEY= '{access_secret}'"
                token = s3.aws_session_token
                if token:
                    credentials_part = credentials_part + f" SESSION_TOKEN = '{token}'"

        sql = dedent(f"""
                    CREATE MULTISET TABLE {teradata_table} AS
                    (
                        SELECT * FROM (
                            LOCATION = '{s3_source_key}'
                            {credentials_part}
                        ) AS d
                    ) WITH DATA
                    """).rstrip()

        self.execute_queries(sql)


    @public
    def azure_blob_to_teradata(
            azure_blob: AzureBlobResource,
            blob_source_key: str,
            teradata_table: str,
            public_bucket: bool = False,
            teradata_authorization_name: str = ""
    ):
        """Loads CSV, JSON, and Parquet format data from Azure Blob Storage to Teradata.

        Args:
            :param blob_source_key: The URI format specifying the location of the Azure blob object store.
                The URI format is `/az/YOUR-STORAGE-ACCOUNT.blob.core.windows.net/YOUR-CONTAINER/YOUR-BLOB-LOCATION`.
                Refer to
                https://docs.teradata.com/search/documents?query=native+object+store&sort=last_update&virtual-field=title_only&content-lang=en-US
            :param public_bucket: Specifies whether the provided blob container is public. If the blob container is public,
                it means that anyone can access the objects within it via a URL without requiring authentication.
                If the container is private and authentication is not provided, the function will raise an exception.
            :param teradata_table: The name of the Teradata table to which the data is transferred.
            :param teradata_authorization_name: The name of Teradata Authorization Database Object,
                is used to control who can access an Azure Blob object store.
                Refer to
                https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/Teradata-VantageTM-Native-Object-Store-Getting-Started-Guide-17.20/Setting-Up-Access/Controlling-Foreign-Table-Access-with-an-AUTHORIZATION-Object
        """

        credentials_part = "ACCESS_ID= '' ACCESS_KEY= ''"

        if not public_bucket:
            # Accessing data directly from the Azure Blob Storage and creating permanent table inside the database
            if teradata_authorization_name:
                credentials_part = f"AUTHORIZATION={teradata_authorization_name}"
            else:
                # Obtaining Azure client ID and secret from the azure_blob resource
                access_id = azure_blob.azure_client_id
                access_secret = azure_blob.azure_client_secret
                credentials_part = f"ACCESS_ID= '{access_id}' ACCESS_KEY= '{access_secret}'"

        sql = dedent(f"""
                    CREATE MULTISET TABLE {teradata_table} AS
                    (
                        SELECT * FROM (
                            LOCATION = '{blob_source_key}'
                            {credentials_part}
                        ) AS d
                    ) WITH DATA
                    """).rstrip()

        self.execute_queries(sql)

@dagster_maintained_resource
@resource(
    config_schema=TeradataResource.to_config_schema(),
    description="This resource is for connecting to the Teradata Vantage",
)


def teradata_resource(context) -> TeradataConnection:
    """A resource for connecting to the Teradata Vantage. The returned resource object is an
    instance of :py:class:`TeradataConnection`.

    A simple example of loading data into Teradata and subsequently querying that data is shown below:

    Examples:
        .. code-block:: python

            from dagster import job, op
            from dagster_teradata import teradata_resource

            @op(required_resource_keys={'teradata'})
            def get_one(context):
                context.resources.teradata.execute_query('SELECT 1')

            @job(resource_defs={'teradata': teradata_resource})
            def my_teradata_job():
                get_one()

            my_teradata_job.execute_in_process(
                run_config={
                    'resources': {
                        'teradata': {
                            'config': {
                                'host': {'env': 'TERADATA_HOST'},
                                'user': {'env': 'TERADATA_USER'},
                                'password': {'env': 'TERADATA_PASSWORD'},
                                'database': {'env': 'TERADATA_DATABASE'},
                            }
                        }
                    }
                }
            )
    """
    teradata_resource = TeradataResource.from_resource_context(context)
    return TeradataConnection(
        config=context, log=context.log, teradata_connection_resource=teradata_resource
    )


def fetch_last_updated_timestamps(
        *,
        teradata_connection: Union[SqlDbConnection, teradatasql.TeradataConnection],
        tables: Sequence[str],
        database: Optional[str] = None,
) -> Mapping[str, datetime]:
    """Fetch the last updated times of a list of tables in Teradata.

    If the underlying query to fetch the last updated time returns no results, a ValueError will be raised.

    Args:
        teradata_connection (Union[SqlDbConnection, TeradataConnection]): A connection to Teradata.
            Accepts either a TeradataConnection or a sqlalchemy connection object,
            which are the two types of connections emittable from the teradata resource.
        schema (str): The schema of the tables to fetch the last updated time for.
        tables (Sequence[str]): A list of table names to fetch the last updated time for.
        database (Optional[str]): The database of the table. Only required if the connection
            has not been set with a database.

    Returns:
        Mapping[str, datetime]: A dictionary of table names to their last updated time in UTC.
    """
    check.invariant(len(tables) > 0, "Must provide at least one table name to query upon.")
    tables_str = ", ".join([f"'{table_name}'" for table_name in tables])
    fully_qualified_table_name = "DBC.TablesV"

    query = f"""
    SELECT TableName, CAST(LastAlterTimestamp AS TIMESTAMP(6)) AS LastAltered
    FROM {fully_qualified_table_name}
    WHERE DatabaseName = '{database}' AND TableName IN ({tables_str});
    """
    result = teradata_connection.cursor().execute(query)
    if not result:
        raise ValueError("No results returned from Teradata update time query.")

    result_mapping = {TableName: LastAltered for TableName, LastAltered in result}
    result_correct_case = {}
    for table_name in tables:
        if table_name not in result_mapping:
            raise ValueError(f"Table {table_name} could not be found.")
        last_altered = result_mapping[table_name]
        check.invariant(
            isinstance(last_altered, datetime),
            "Expected last_altered to be a datetime, but it was not.",
        )
        result_correct_case[table_name] = last_altered

    return result_correct_case