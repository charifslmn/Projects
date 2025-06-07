import shutil
from pathlib import Path
from typing import Any, Dict
from hatchling.builders.hooks.plugin.interface import BuildHookInterface


class CustomBuildHook(BuildHookInterface):
    def finalize(self, version: str, build_data: Dict[str, Any], artifact_path: str) -> None:
        path = Path(artifact_path)
        if path.suffix == ".whl":  # only do this for wheel builds
            root_level = path.parent.parent
            sf_build = root_level / "sf_build"
            sf_build.mkdir(exist_ok=True)

            # Copy and rename the wheel file to a .zip
            shutil.copyfile(path, sf_build / (self.metadata.name.replace("-", "_") + ".zip"))
            
            # Copy additional files needed for Snowflake
            file_to_copy = ["setup.sql", "manifest.yml", "streamlit_app.py", "environment.yml", "logo.png","common.py"]
            for file in file_to_copy:
                shutil.copyfile(root_level / file, sf_build / file)
