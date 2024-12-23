from pathlib import Path

import dagster as dg
from dagster._components import ComponentRegistry, build_defs_from_toplevel_components_folder
from dagster._components.impls.pipes_subprocess_script_collection import (
    PipesSubprocessScriptCollection,
)

defs = build_defs_from_toplevel_components_folder(
    path=Path(__file__).parent,
    registry=ComponentRegistry(
        {"pipes_subprocess_script_collection": PipesSubprocessScriptCollection}
    ),
)

if __name__ == "__main__":
    dg.Definitions.validate_loadable(defs)
