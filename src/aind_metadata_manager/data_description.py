"""Upgrade legacy data descriptions before deriving metadata."""

import logging
from copy import deepcopy

from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import DataDescription
from aind_metadata_upgrader.data_description.v1v2 import DataDescriptionV1V2
from aind_metadata_upgrader.utils.v1v2_utils import upgrade_registry

logger = logging.getLogger(__name__)


def _upgrade_person(person: dict) -> Person:
    """Pass a Person to the upgrader so it retains registry identifiers."""
    upgraded = upgrade_registry(person)
    upgraded.pop("abbreviation", None)
    if upgraded.get("registry") is None:
        upgraded.pop("registry", None)
    return Person(**upgraded)


def upgrade_data_description(data: dict) -> DataDescription:
    """Validate a v2 description, converting schema v1 inputs in memory."""
    if not str(data.get("schema_version", "")).startswith("1."):
        return DataDescription(**data)

    source = deepcopy(data)
    source["investigators"] = [
        _upgrade_person(person) for person in source["investigators"]
    ]
    upgraded = DataDescriptionV1V2().upgrade(
        source,
        schema_version=DataDescription.model_fields["schema_version"].default,
    )
    if data.get("label"):
        upgraded["tags"] = [data["label"]]

    if not data.get("project_name"):
        logger.warning(
            "Schema v1 data description has no project_name; "
            "using 'unknown' for the required v2 field"
        )
    if data.get("related_data"):
        logger.warning(
            "Schema v1 related_data has no v2 equivalent and is not "
            "copied; it remains in the input data description"
        )

    return DataDescription(**upgraded)
