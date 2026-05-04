"""Version-aware utilities for reading aind-data-schema metadata.

Provides a central major version check from data_description.json and
version-aware file resolution for v1/v2 of aind-data-schema.
"""

import json
from enum import Enum
from pathlib import Path
from typing import Union


class SchemaVersion(str, Enum):
    """Major schema version of aind-data-schema."""

    V1 = "v1"
    V2 = "v2"

    def __str__(self) -> str:
        return self.value


class CoreFilename(str, Enum):
    """Standard filenames for core aind-data-schema metadata."""

    # Shared across v1 and v2
    DATA_DESCRIPTION = "data_description.json"
    SUBJECT = "subject.json"
    PROCEDURES = "procedures.json"
    PROCESSING = "processing.json"
    QUALITY_CONTROL = "quality_control.json"

    # v2 names (v1 equivalents: session.json, rig.json)
    ACQUISITION = "acquisition.json"
    INSTRUMENT = "instrument.json"

    # v1 names (renamed in v2)
    SESSION = "session.json"
    RIG = "rig.json"

    def __str__(self) -> str:
        return self.value


def _load_json(source: Union[dict, str, Path]) -> dict:
    """Load a JSON file path into a dict, or pass through an existing dict."""
    if isinstance(source, (str, Path)):
        with open(source) as f:
            return json.load(f)
    return source


def get_major_schema_version(
    data_description: Union[dict, str, Path],
) -> SchemaVersion:
    """Determine aind-data-schema major version from data_description.

    Parameters
    ----------
    data_description : dict, str, or Path
        Parsed contents of data_description.json, OR a file path
        (str/Path) which will be loaded automatically.

    Returns
    -------
    major_version_str : SchemaVersion
        ``SchemaVersion.V2`` if schema_version starts with
        ``"2."``, ``SchemaVersion.V1`` otherwise (including
        missing).
    """
    data = _load_json(data_description)
    schema_version = (
        data.get("schema_version", "") or ""
    )
    if schema_version.startswith("2."):
        return SchemaVersion.V2
    return SchemaVersion.V1


def get_metadata(
    input_dir: Path,
    filename: Union[str, CoreFilename],
) -> dict:
    """Extract metadata from a JSON file by recursive search.

    Parameters
    ----------
    input_dir : Path
        Input directory to search recursively.
    filename : str or CoreFilename
        Filename or glob pattern to search for
        (e.g. ``"subject.json"`` or
        ``CoreFilename.SUBJECT``).

    Returns
    -------
    metadata : dict
        Parsed JSON contents.

    Raises
    ------
    FileNotFoundError
        If no matching file is found in ``input_dir``.
    """
    # str(filename) allows both str and CoreFilename to be used
    input_fp = next(input_dir.rglob(str(filename)), "")
    if not input_fp:
        raise FileNotFoundError(
            f"No {filename} file found in {input_dir}"
        )
    with open(input_fp, "r") as f:
        metadata = json.load(f)
    return metadata


def get_acquisition_metadata(
    input_dir: Path,
    major_version_str: SchemaVersion,
) -> dict:
    """Load acquisition.json (v2) or session.json (v1).

    Parameters
    ----------
    input_dir : Path
        Directory containing the metadata file.
    major_version_str : SchemaVersion
        ``SchemaVersion.V2`` loads ``acquisition.json``,
        ``SchemaVersion.V1`` loads ``session.json``.

    Returns
    -------
    metadata : dict
        Parsed JSON contents.
    """
    filename = (
        CoreFilename.ACQUISITION
        if major_version_str == SchemaVersion.V2
        else CoreFilename.SESSION
    )
    return get_metadata(input_dir, filename)
