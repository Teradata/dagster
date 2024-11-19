from abc import abstractmethod
from contextlib import contextmanager
from typing import Optional, Sequence, Type, cast

from dagster import IOManagerDefinition, OutputContext, io_manager
from dagster._config.pythonic_config import ConfigurableIOManagerFactory
from dagster._core.definitions.time_window_partitions import TimeWindow
from dagster._core.storage.db_io_manager import (
    DbClient,
    DbIOManager,
    DbTypeHandler,
    TablePartitionDimension,
    TableSlice,
)
from dagster._core.storage.io_manager import dagster_maintained_io_manager
from pydantic import Field
from teradatasql import ProgrammingError

from dagster_teradata.resources import TeradataResource

TERADATA_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def build_teradata_io_manager(
    type_handlers: Sequence[DbTypeHandler], default_load_type: Optional[Type] = None
) -> IOManagerDefinition:
    @dagster_maintained_io_manager
    @io_manager(config_schema=TeradataIOManager.to_config_schema())
    def teradata_io_manager(init_context):
        return DbIOManager(
            type_handlers=type_handlers,
            db_client=TeradataDbClient(),
            io_manager_name="TeradataIOManager",
            database=init_context.resource_config["database"],
            schema=init_context.resource_config.get("schema"),
            default_load_type=default_load_type,
        )

    return teradata_io_manager


class TeradataIOManager(ConfigurableIOManagerFactory):
    host: str = Field(description="Hostname of Teradata database")
    user: str = Field(description="User login name.")
    database: str = Field(description="Name of the database to use.")
    password: Optional[str] = Field(default=None, description="User password.")

    @staticmethod
    @abstractmethod
    def type_handlers() -> Sequence[DbTypeHandler]: ...

    @staticmethod
    def default_load_type() -> Optional[Type]:
        return None

    def create_io_manager(self, context) -> DbIOManager:
        return DbIOManager(
            db_client=TeradataDbClient(),
            io_manager_name="TeradataIOManager",
            database=self.database,
            type_handlers=self.type_handlers(),
            default_load_type=self.default_load_type(),
        )


class TeradataDbClient(DbClient):
    @staticmethod
    @contextmanager
    def connect(context, table_slice):
        no_schema_config = (
            {k: v for k, v in context.resource_config.items() if k != "schema"}
            if context.resource_config
            else {}
        )
        with TeradataResource(schema=table_slice.schema, **no_schema_config).get_connection(
            raw_conn=False
        ) as conn:
            yield conn

    @staticmethod
    def delete_table_slice(context: OutputContext, table_slice: TableSlice, connection) -> None:
        try:
            connection.cursor().execute(_get_cleanup_statement(table_slice))
        except ProgrammingError as e:
            if "does not exist" in e.msg:  # type: ignore
                # table doesn't exist yet, so ignore the error
                return
            else:
                raise

    @staticmethod
    def get_select_statement(table_slice: TableSlice) -> str:
        col_str = ", ".join(table_slice.columns) if table_slice.columns else "*"
        if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
            query = f"SELECT {col_str} FROM" f" {table_slice.database}.{table_slice.table} WHERE\n"
            return query + _partition_where_clause(table_slice.partition_dimensions)
        else:
            return f"""SELECT {col_str} FROM {table_slice.database}.{table_slice.table}"""


def _get_cleanup_statement(table_slice: TableSlice) -> str:
    """Returns a SQL statement that deletes data in the given table to make way for the output data
    being written.
    """
    if table_slice.partition_dimensions and len(table_slice.partition_dimensions) > 0:
        query = f"DELETE FROM {table_slice.database}.{table_slice.table} WHERE\n"
        return query + _partition_where_clause(table_slice.partition_dimensions)
    else:
        return f"DELETE FROM {table_slice.database}.{table_slice.table}"


def _partition_where_clause(partition_dimensions: Sequence[TablePartitionDimension]) -> str:
    return " AND\n".join(
        (
            _time_window_where_clause(partition_dimension)
            if isinstance(partition_dimension.partitions, TimeWindow)
            else _static_where_clause(partition_dimension)
        )
        for partition_dimension in partition_dimensions
    )


def _time_window_where_clause(table_partition: TablePartitionDimension) -> str:
    partition = cast(TimeWindow, table_partition.partitions)
    start_dt, end_dt = partition
    start_dt_str = start_dt.strftime(TERADATA_DATETIME_FORMAT)
    end_dt_str = end_dt.strftime(TERADATA_DATETIME_FORMAT)
    # Teradata BETWEEN is inclusive; start <= partition expr <= end. We don't want to remove the next partition so we instead
    # write this as start <= partition expr < end.
    return f"""{table_partition.partition_expr} >= '{start_dt_str}' AND {table_partition.partition_expr} < '{end_dt_str}'"""


def _static_where_clause(table_partition: TablePartitionDimension) -> str:
    partitions = ", ".join(f"'{partition}'" for partition in table_partition.partitions)
    return f"""{table_partition.partition_expr} in ({partitions})"""
