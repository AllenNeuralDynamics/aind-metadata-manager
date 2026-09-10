"""Tests for deriving v2 descriptions from schema v1 metadata."""

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from aind_data_schema.core.data_description import DataDescription
from aind_data_schema_models.data_name_patterns import DataLevel
from aind_data_schema_models.modalities import Modality
from aind_data_schema_models.registries import Registry
from aind_metadata_upgrader.data_description.v1v2 import DataDescriptionV1V2
from pydantic import ValidationError

from aind_metadata_manager.metadata_manager import (
    MetadataManager,
    MetadataSettings,
)


class TestDataDescriptionUpgrade(unittest.TestCase):
    """Exercise real schema validation and the public manager entry point."""

    def setUp(self):
        """Load a description matching aind-data-schema v1.4's model."""
        path = (
            Path(__file__).parent
            / "resources"
            / "data_description_examples"
            / "data_description_1.0.4.json"
        )
        self.data = json.loads(path.read_text())

    def _derive(self, data, **overrides):
        """Derive, reload and validate an output without changing the input."""
        with tempfile.TemporaryDirectory() as directory:
            input_dir = Path(directory) / "input" / "asset"
            input_dir.mkdir(parents=True)
            output_dir = Path(directory) / "output"
            output_dir.mkdir()
            input_path = input_dir / "data_description.json"
            original = json.dumps(data)
            input_path.write_text(original)
            settings = MetadataSettings(
                _cli_parse_args=False,
                input_dir=input_dir.parent,
                output_dir=output_dir,
                processor_full_name="Test User",
                pipeline_url="https://example.com/pipeline",
                process_name="sorted",
                **overrides,
            )
            MetadataManager(settings).create_derived_data_description()
            self.assertEqual(input_path.read_text(), original)
            return DataDescription.model_validate_json(
                (output_dir / "data_description.json").read_text()
            )

    def test_raw_v1_versions(self):
        """Both ends of the v1 library series produce a fresh v2 asset."""
        for version in ("1.0.0", "1.0.4"):
            with (
                self.subTest(schema_version=version),
                mock.patch(
                    "aind_metadata_upgrader.data_description.v1v2."
                    "_get_parent_data_description",
                    side_effect=AssertionError("Unexpected ancestry lookup"),
                ),
            ):
                self.data["schema_version"] = version
                before = datetime.now(timezone.utc)
                derived = self._derive(self.data)
                self.assertEqual(derived.data_level, DataLevel.DERIVED)
                self.assertEqual(
                    derived.schema_version,
                    DataDescription.model_fields["schema_version"].default,
                )
                self.assertTrue(derived.schema_version.startswith("2."))
                self.assertTrue(
                    derived.name.startswith(self.data["name"] + "_sorted_")
                )
                self.assertGreaterEqual(derived.creation_time, before)
                self.assertEqual(derived.subject_id, self.data["subject_id"])
                self.assertEqual(
                    derived.modalities, [Modality.ECEPHYS, Modality.BEHAVIOR]
                )
                self.assertEqual(derived.data_summary, "Original summary")
                self.assertEqual(derived.project_name, "Example project")
                self.assertEqual(derived.restrictions, "Internal use")
                self.assertEqual(derived.source_data, [self.data["name"]])

    def test_identifiers_and_funding(self):
        """Registry conversion preserves people, IDs, grants and fundees."""
        upgraded = self._derive(self.data)
        self.assertEqual(upgraded.institution.registry, Registry.ROR)
        self.assertEqual(upgraded.institution.registry_identifier, "04szwah67")
        investigator = upgraded.investigators[0]
        self.assertEqual(investigator.registry, Registry.ORCID)
        self.assertEqual(
            investigator.registry_identifier, "0000-0002-1825-0097"
        )
        funding = upgraded.funding_source[0]
        self.assertEqual(funding.funder.registry, Registry.ROR)
        self.assertEqual(funding.grant_number, "test-grant")
        self.assertEqual(funding.fundee[0].name, "Jane Doe")

    def test_missing_project_name(self):
        """Use the upgrader's default for missing legacy projects."""
        for value in (None, ""):
            self.data["project_name"] = value
            with self.subTest(value=value):
                self.assertEqual(
                    self._derive(self.data).project_name, "unknown"
                )
        del self.data["project_name"]
        self.assertEqual(self._derive(self.data).project_name, "unknown")

    def test_derived_v1_input(self):
        """A previously derived v1 asset uses the existing v2 naming flow."""
        raw_name = self.data["name"]
        self.data.update(
            data_level="derived",
            input_data_name=raw_name,
            process_name="processed",
            creation_time="2024-03-25T11:22:44Z",
            name=raw_name + "_processed_2024-03-25_11-22-44",
        )
        derived = self._derive(self.data)
        self.assertEqual(derived.data_level, DataLevel.DERIVED)
        self.assertTrue(derived.name.startswith(raw_name + "_sorted_"))
        self.assertNotEqual(derived.name, self.data["name"])
        self.assertIn(self.data["name"], derived.source_data)

    def test_overrides_after_conversion(self):
        """Existing overrides are applied to the converted description."""
        derived = self._derive(
            self.data, modality="pophys", data_summary="Processed summary"
        )
        self.assertEqual(derived.modalities, [Modality.POPHYS])
        self.assertEqual(derived.data_summary, "Processed summary")

    def test_deep_derived_input_without_lookups(self):
        """Create required v2 output without resolving legacy ancestry."""
        raw_name = self.data["name"]
        parent_name = raw_name + "_processed_2024-03-25_11-22-44"
        self.data.update(
            data_level="derived",
            input_data_name=parent_name,
            process_name="reviewed",
            name=parent_name + "_reviewed_2024-03-26_11-22-44",
        )
        with (
            mock.patch(
                "aind_metadata_upgrader.data_description.v1v2."
                "_get_parent_data_description",
                side_effect=AssertionError("Unexpected ancestry lookup"),
            ),
            mock.patch(
                "aind_metadata_upgrader.data_description.v1v2."
                "client.retrieve_docdb_records",
                side_effect=AssertionError("Unexpected network call"),
            ),
        ):
            derived = self._derive(self.data)
        self.assertEqual(derived.data_level, DataLevel.DERIVED)
        self.assertEqual(derived.source_data, [self.data["name"]])

    def test_v2_input_unchanged(self):
        """V2 inputs follow the same validation and derivation path."""
        v2 = self._derive(self.data)
        data = v2.model_dump(mode="json")
        with mock.patch.object(
            DataDescriptionV1V2,
            "upgrade",
            side_effect=AssertionError("Unexpected v2 upgrade"),
        ):
            self.assertEqual(self._derive(data).modalities, v2.modalities)

    def test_invalid_inputs_raise(self):
        """Validate the upgrader's output against the installed schema."""
        for field, value in (
            ("creation_time", "invalid"),
            ("data_level", "invalid"),
            ("subject_id", "invalid_subject"),
        ):
            data = {**self.data, field: value}
            with self.subTest(field=field):
                with self.assertRaises(ValidationError):
                    self._derive(data)

    def test_unknown_modality_raises(self):
        """Propagate unsupported modality errors from the upgrader."""
        self.data["modality"][0]["abbreviation"] = "not a modality"
        with self.assertRaises(ValueError):
            self._derive(self.data)

    def test_upgrader_errors_propagate(self):
        """Do not fall back when the upstream conversion fails."""
        with mock.patch.object(
            DataDescriptionV1V2, "upgrade", side_effect=ValueError("failed")
        ):
            with self.assertRaisesRegex(ValueError, "failed"):
                self._derive(self.data)


if __name__ == "__main__":
    unittest.main()
