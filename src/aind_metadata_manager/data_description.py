"""Local conversion of schema v1 data descriptions to v2."""

import logging

from aind_data_schema.components.identifiers import Person
from aind_data_schema.core.data_description import DataDescription
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.registries import Registry

logger = logging.getLogger(__name__)


def _upgrade_registry(identifier: dict) -> dict:
    """Convert a v1 registry object without changing its identifier."""
    upgraded = identifier.copy()
    registry = upgraded.get("registry")
    if isinstance(registry, dict):
        abbreviation = registry.get("abbreviation")
        if abbreviation not in Registry.__members__:
            raise ValueError(f"Unsupported registry: {registry}")
        upgraded["registry"] = Registry[abbreviation]
    return upgraded


def _upgrade_person(person: dict) -> Person:
    """Convert a v1 PIDName to a Person, preserving registered IDs."""
    upgraded = _upgrade_registry(person)
    upgraded.pop("abbreviation", None)
    if upgraded.get("registry") is None:
        upgraded.pop("registry", None)
    return Person(**upgraded)


def _upgrade_modality(modality: dict) -> dict:
    """Resolve a legacy modality to its current controlled vocabulary."""
    for modality_class in Modality.ALL:
        canonical = modality_class()
        if canonical.name == modality["name"]:
            return canonical.model_dump()
    raise ValueError(f"Unsupported v1 modality: {modality}")


def upgrade_data_description(data: dict) -> DataDescription:
    """Validate a v2 description, converting schema v1 inputs in memory."""
    if not str(data.get("schema_version", "")).startswith("1."):
        return DataDescription(**data)

    upgraded = data.copy()
    for field in (
        "schema_version",
        "describedBy",
        "platform",
        "related_data",
        "process_name",
        "analysis_name",
    ):
        upgraded.pop(field, None)
    label = upgraded.pop("label", None)
    if label:
        upgraded["tags"] = [label]

    input_data_name = upgraded.pop("input_data_name", None)
    if input_data_name and upgraded.get("data_level") == "derived":
        upgraded["source_data"] = [input_data_name]

    upgraded["institution"] = _upgrade_registry(upgraded["institution"])
    upgraded["investigators"] = [
        _upgrade_person(person) for person in upgraded["investigators"]
    ]
    funding_sources = []
    for funding in upgraded["funding_source"]:
        funding = funding.copy()
        funding["funder"] = _upgrade_registry(funding["funder"])
        if funding.get("fundee") is not None:
            funding["fundee"] = [Person(name=funding["fundee"])]
        funding_sources.append(funding)
    upgraded["funding_source"] = funding_sources

    upgraded["modalities"] = [
        _upgrade_modality(modality) for modality in upgraded.pop("modality")
    ]

    if not upgraded.get("project_name"):
        logger.warning(
            "Schema v1 data description has no project_name; "
            "using 'unknown' for the required v2 field"
        )
        upgraded["project_name"] = "unknown"
    if data.get("related_data"):
        logger.warning(
            "Schema v1 related_data has no v2 equivalent and is not "
            "copied; it remains in the input data description"
        )

    return DataDescription(**upgraded)
