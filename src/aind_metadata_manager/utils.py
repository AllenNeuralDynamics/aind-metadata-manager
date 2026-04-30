"""Version-aware utilities for reading aind-data-schema metadata.

Provides a central major version check from data_description.json and
compatible field accessors that pull values from the correct JSON path
depending on whether the dataset uses v1 or v2 of aind-data-schema.
"""

import json
from enum import Enum
from pathlib import Path
from typing import List, Union


class SchemaVersion(str, Enum):
    """Major schema version of aind-data-schema."""

    V1 = "v1"
    V2 = "v2"


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


def _get_frame_rate_v1(data: dict) -> List[float]:
    """v1 path: data_streams[].ophys_fovs[].frame_rate."""
    frame_rates: List[float] = []
    for stream in data.get("data_streams", []):
        for fov in stream.get("ophys_fovs", []):
            if "frame_rate" in fov:
                frame_rates.append(
                    float(fov["frame_rate"])
                )
    return frame_rates


def _get_frame_rate_v2(data: dict) -> List[float]:
    """v2 path: data_streams[].configurations[].sampling_strategy.frame_rate."""
    frame_rates: List[float] = []
    for stream in data.get("data_streams", []):
        for config in stream.get("configurations", []):
            strategy = config.get(
                "sampling_strategy", {}
            )
            if strategy and "frame_rate" in strategy:
                frame_rates.append(
                    float(strategy["frame_rate"])
                )
    return frame_rates


def get_compatible_frame_rate(
    acquisition_data: Union[dict, str, Path],
    major_version_str: SchemaVersion,
) -> List[float]:
    """Extract frame rates from session (v1) or acquisition (v2) metadata.

    Parameters
    ----------
    acquisition_data : dict, str, or Path
        Parsed session.json (v1) or acquisition.json (v2),
        or a file path which will be loaded automatically.
    major_version_str : SchemaVersion
        ``SchemaVersion.V1`` or ``SchemaVersion.V2`` from
        :func:`get_major_schema_version`.

    Returns
    -------
    frame_rates : list of float
        All frame_rate values found across data streams.
    """
    data = _load_json(acquisition_data)
    if major_version_str == SchemaVersion.V2:
        return _get_frame_rate_v2(data)
    return _get_frame_rate_v1(data)
