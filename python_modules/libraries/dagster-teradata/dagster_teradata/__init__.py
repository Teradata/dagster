from dagster._core.libraries import DagsterLibraryRegistry

from dagster_teradata.resources import (
    TeradataConnection as TeradataConnection,
    TeradataResource as TeradataResource,
    fetch_last_updated_timestamps as fetch_last_updated_timestamps,
    teradata_resource as teradata_resource,
)

from dagster_teradata.version import __version__

DagsterLibraryRegistry.register("dagster-teradata", __version__)

