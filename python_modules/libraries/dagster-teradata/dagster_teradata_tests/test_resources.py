import os
import uuid
import pandas as pd
import teradatasql
import pytest
from unittest import mock
from contextlib import contextmanager
from typing import Iterator
from dagster import (
    DagsterInstance,
    DagsterResourceFunctionError,
    DataVersion,
    EnvVar,
    ObserveResult,
    build_resources,
    job,
    observable_source_asset,
    op,
    asset,
    materialize,
)
from dagster._time import get_current_timestamp
from dagster_aws.s3 import S3Resource
from dagster_teradata import TeradataResource, fetch_last_updated_timestamps, teradata_resource

@pytest.mark.integration
def test_resource(tmp_path):
    df = ['a']

    @asset
    def drop_table(teradata: TeradataResource):
        try:
            with teradata.get_connection() as conn:
                conn.cursor().execute("DROP TABLE dbcinfo;")
        except teradatasql.DatabaseError as e:
            # Error code 3807 corresponds to "table does not exist" in Teradata
            if "3807" in str(e):
                print("Table dbcinfo does not exist, ignoring error 3807.")
            else:
                # Re-raise the exception if it's not error 3807
                raise


    @asset
    def create_table(teradata: TeradataResource, drop_table):
        with teradata.get_connection() as conn:
            conn.cursor().execute("CREATE TABLE dbcinfo (infokey varchar(50));")


    @asset
    def insert_rows(teradata: TeradataResource, create_table):
        with teradata.get_connection() as conn:
            conn.cursor().execute("insert into dbcinfo (infokey) values ('a');")

    @asset
    def read_table(teradata: TeradataResource, insert_rows):
        with teradata.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("select * from dbcinfo;")
            res = cursor.fetchall()
            result_list = [row[0] for row in res]
            assert result_list==df


    materialize(
        [drop_table, create_table, insert_rows, read_table],
        resources={"teradata": TeradataResource(
            host=os.getenv("TERADATA_HOST"),
            user=os.getenv("TERADATA_USER"),
            password=os.getenv("TERADATA_PASSWORD"),
            database=os.getenv("TERADATA_DATABASE"))},
    )

@pytest.mark.integration
def test_resources_teradata_connection():
    with TeradataResource(
        host=os.getenv("TERADATA_HOST"),
        user=os.getenv("TERADATA_USER"),
        password=os.getenv("TERADATA_PASSWORD"),
        database=os.getenv("TERADATA_DATABASE"),
    ).get_connection() as conn:
        # Teradata table names are expected to be capitalized.
        table_name = f"test_table_{str(uuid.uuid4()).replace('-', '_')}".lower()
        try:
            start_time = get_current_timestamp()
            conn.cursor().execute(f"create table {table_name} (foo varchar(10))")
            # Insert one row
            conn.cursor().execute(f"insert into {table_name} values ('bar')")

            freshness_for_table = fetch_last_updated_timestamps(
                teradata_connection=conn,
                database=os.getenv("TERADATA_DATABASE"),
                tables=[
                    table_name
                ],  # Teradata table names are expected uppercase. Test that lowercase also works.
            )[table_name].timestamp()

            end_time = get_current_timestamp()

            assert freshness_for_table > start_time
        finally:
            try:
                conn.cursor().execute(f"drop table if exists {table_name}")
            except Exception as ex:
                ignored = False
                if f"[Error 3807]" in str(ex):
                    ignored = True
