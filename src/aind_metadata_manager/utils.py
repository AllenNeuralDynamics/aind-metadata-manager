"""Version-aware utilities for reading aind-data-schema metadata.

Provides a central major version check from data_description.json and
version-aware file resolution for v1/v2 of aind-data-schema.

Core filenames are sourced from the installed aind-data-schema models
(via ``default_filename()``) so they track the schema package. v1-only
names (renamed in v2) are retained as literals because the v2 package no
longer defines those models.
"""

import json
from enum import Enum
from pathlib import Path
from typing import Union

from aind_data_schema.core.acquisition import Acquisition
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema.core.instrument import Instrument
from aind_data_schema.core.procedures import Procedures
from aind_data_schema.core.processing import Processing
from aind_data_schema.core.quality_control import QualityControl
from aind_data_schema.core.subject import Subject
from pydantic import BaseModel


class SchemaVersion(str, Enum):
    """Major schema version of aind-data-schema."""

    V1 = "v1"
    V2 = "v2"

    def __str__(self) -> str:
        return self.value


class CoreFilename(str, Enum):
    """Standard filenames for core aind-data-schema metadata.

    v2 names are sourced from each core model's ``default_filename()``
    so they track the installed aind-data-schema. v1-only names
    (session.json, rig.json) are retained as literals because the v2
    package no longer defines those models.
    """

    # v2 names — sourced from the installed aind-data-schema models
    DATA_DESCRIPTION = DataDescription.default_filename()
    SUBJECT = Subject.default_filename()
    PROCEDURES = Procedures.default_filename()
    PROCESSING = Processing.default_filename()
    QUALITY_CONTROL = QualityControl.default_filename()
    ACQUISITION = Acquisition.default_filename()
    INSTRUMENT = Instrument.default_filename()

    # v1-only names (renamed in v2; not defined by the v2 package)
    SESSION = "session.json"
    RIG = "rig.json"

    def __str__(self) -> str:
        return self.value


def _load_json(source: Union[dict, str, Path, BaseModel]) -> dict:
    """Load a dict from a JSON path, a pydantic model, or a dict.

    Parameters
    ----------
    source : dict, str, Path, or pydantic.BaseModel
        A file path to load, a parsed aind-data-schema model to dump,
        or an already-parsed dict to pass through.

    Returns
    -------
    data : dict
        The metadata as a plain dict.
    """
    if isinstance(source, BaseModel):
        return source.model_dump(mode="json")
    if isinstance(source, (str, Path)):
        with open(source, encoding="utf-8") as f:
            return json.load(f)
    return source


def get_major_schema_version(
    data_description: Union[dict, str, Path, DataDescription],
) -> SchemaVersion:
    """Determine aind-data-schema major version from data_description.

    Parameters
    ----------
    data_description : dict, str, Path, or DataDescription
        Parsed contents of data_description.json, a parsed
        ``DataDescription`` model, OR a file path (str/Path) which
        will be loaded automatically. Note that a parsed
        ``DataDescription`` reports the installed schema version, since
        aind-data-schema coerces ``schema_version`` on construction.

    Returns
    -------
    major_version_str : SchemaVersion
        ``SchemaVersion.V2`` if schema_version starts with
        ``"2."``, ``SchemaVersion.V1`` otherwise (including
        missing, null, or non-string values).
    """
    data = _load_json(data_description)
    schema_version = str(data.get("schema_version") or "")
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
    input_fp = next(input_dir.rglob(str(filename)), None)
    if input_fp is None:
        raise FileNotFoundError(f"No {filename} file found in {input_dir}")
    with open(input_fp, "r", encoding="utf-8") as f:
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
