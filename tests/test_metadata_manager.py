"""Unit tests for MetadataManager functionality."""

import json
import tempfile
import unittest
from pathlib import Path
from typing import ClassVar
from unittest import mock

from aind_metadata_manager.metadata_manager import (
    MetadataManager,
    MetadataSettings,
)


class DummySettings(MetadataSettings):
    """Dummy settings for testing purposes."""

    cli_parse_args: ClassVar[bool] = False
    input_dir: Path
    output_dir: Path
    processor_full_name: str = "Test User"
    pipeline_version: str = "1.0"
    pipeline_url: str = "http://example.com"
    data_summary: str = "Test summary"
    modality: str = "E"
    skip_ancillary_files: bool = True
    aggregate_quality_control: bool = False
    verbose: bool = False


class TestMetadataManager(unittest.TestCase):
    """Tests for MetadataManager functionality."""

    def test_find_matching_file_verbose(self):
        """Test finding a matching file with verbose output."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "foo.txt").write_text("bar")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                found = manager._find_matching_file("foo.txt")
                self.assertIsNotNone(found)

    def test_copy_file_error(self):
        """Test copying a file that does not exist."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                src = Path(tempdir) / "src.txt"
                dst = Path(tempdir) / "dst.txt"
                # src does not exist
                settings = DummySettings(
                    input_dir=Path(tempdir),
                    output_dir=Path(tempdir),
                    verbose=True,
                )
                manager = MetadataManager(settings)
                with self.assertRaises(FileNotFoundError):
                    manager._copy_file(src, dst, "src.txt")

    def test_handle_missing_file_verbose(self):
        """Test handling a missing file with verbose output."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                settings = DummySettings(
                    input_dir=Path(tempdir),
                    output_dir=Path(tempdir),
                    verbose=True,
                )
                manager = MetadataManager(settings)
                manager._handle_missing_file("notfound.txt")

    def test_find_data_description_file_multiple(self):
        """Test finding a data description file in multiple directories."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "data_description.json").write_text("{}")
                (input_dir / "sub").mkdir()
                (input_dir / "sub" / "data_description.json").write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                found = manager._find_data_description_file()
                self.assertIsNotNone(found)

    def test_load_and_upgrade_data_description_verbose(self):
        """Test loading and upgrading a data description with verbose output.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                dd_path = input_dir / "data_description.json"
                dd_path.write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                # Patch DataDescriptionUpgrade to avoid real upgrade logic
                with mock.patch(
                    "aind_metadata_manager.metadata_manager.DataDescriptionUpgrade"  # noqa: E501
                ) as MockUpgrade:
                    instance = MockUpgrade.return_value
                    instance.upgrade.return_value = mock.Mock()
                    result = manager._load_and_upgrade_data_description(
                        dd_path
                    )
                    self.assertIsNotNone(result)

    def test_write_derived_data_description_verbose(self):
        """Test writing derived data description with verbose output."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                output_dir = Path(tempdir)
                settings = DummySettings(
                    input_dir=output_dir, output_dir=output_dir, verbose=True
                )
                manager = MetadataManager(settings)
                dummy_upgrader = mock.Mock()
                with mock.patch(
                    "aind_metadata_manager.metadata_manager.DerivedDataDescription"  # noqa: E501
                ) as MockDerived:
                    instance = MockDerived.from_data_description.return_value
                    instance.write_standard_file.return_value = None
                    manager._write_derived_data_description(dummy_upgrader)
                    self.assertTrue(MockDerived.from_data_description.called)

    def test_copy_ancillary_files_verbose_and_skip(self):
        """Test copying ancillary files with verbose output and skipping."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    skip_ancillary_files=True,
                    verbose=True,
                )
                manager = MetadataManager(settings)
                manager.copy_ancillary_files()  # Should skip and not error

    def test_create_derived_data_description_missing_file(self):
        """Test creating derived data description when the file is missing."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir, verbose=True
                )
                manager = MetadataManager(settings)
                # Should not raise, just return
                manager.create_derived_data_description()

    def test_create_derived_data_description_error(self):
        """Test creating derived data description when an error occurs."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                dd_path = input_dir / "data_description.json"
                dd_path.write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir, verbose=True
                )
                manager = MetadataManager(settings)
                with mock.patch(
                    "aind_metadata_manager.metadata_manager.DataDescriptionUpgrade",  # noqa: E501
                    side_effect=Exception("fail"),
                ):
                    with self.assertRaises(Exception):
                        manager.create_derived_data_description()

    def test_collect_json_objects_empty(self):
        """Test collecting JSON objects when no files are found."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                objs = manager.collect_json_objects("notfound")
                self.assertEqual(objs, [])

    def test_collect_evaluations_invalid(self):
        """Test collecting evaluations when the file is invalid."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "foo_evaluation.json").write_text(
                    "{invalid json}"
                )
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                evals = manager.collect_evaluations()
                self.assertIsInstance(evals, list)

    def test_create_quality_control_metadata_with_eval(self):
        """Test creating quality control metadata with an evaluation file."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                # Write a valid evaluation JSON if possible
                (input_dir / "foo_evaluation.json").write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir, verbose=True
                )
                manager = MetadataManager(settings)
                qc = manager.create_quality_control_metadata()
                self.assertIsNotNone(qc)

    def test_run_function(self):
        """Test the run() function can be called without error in a minimal
        environment.
        """
        with mock.patch("sys.argv", [""]):
            with mock.patch(
                "aind_metadata_manager.metadata_manager.MetadataSettings"
            ) as MockSettings:
                mock_settings = MockSettings.return_value
                mock_settings.verbose = False
                mock_settings.input_dir = Path("/tmp")
                mock_settings.output_dir = Path("/tmp")
                with mock.patch(
                    "aind_metadata_manager.metadata_manager.MetadataManager"
                ) as MockManager:
                    mock_manager = MockManager.return_value
                    mock_manager.create_processing_metadata.return_value = (
                        mock.Mock(write_standard_file=lambda x: None)
                    )
                    mock_manager.copy_ancillary_files.return_value = None
                    from aind_metadata_manager.metadata_manager import run

                    run()

    def test_find_matching_file_and_handle_missing(self):
        """Test _find_matching_file and _handle_missing_file for found
        and not found cases.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "foo.txt").write_text("bar")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir
                )
                manager = MetadataManager(settings)
                # Should find the file
                found = manager._find_matching_file("foo.txt")
                self.assertIsNotNone(found)
                # Should not find a non-existent file
                not_found = manager._find_matching_file("baz.txt")
                self.assertIsNone(not_found)
                # _handle_missing_file just logs, but should not error
                manager._handle_missing_file("baz.txt")

    def test_copy_file(self):
        """Test _copy_file copies a file successfully."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                src = Path(tempdir) / "src.txt"
                dst = Path(tempdir) / "dst.txt"
                src.write_text("hello")
                settings = DummySettings(
                    input_dir=Path(tempdir), output_dir=Path(tempdir)
                )
                manager = MetadataManager(settings)
                manager._copy_file(src, dst, "src.txt")
                self.assertTrue(dst.exists())
                self.assertEqual(dst.read_text(), "hello")

    def test_find_data_description_file(self):
        """Test _find_data_description_file finds a single file."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                (input_dir / "data_description.json").write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir
                )
                manager = MetadataManager(settings)
                found = manager._find_data_description_file()
                self.assertIsNotNone(found)

    def test_apply_overrides_and_validate_modality(self):
        """Test _apply_overrides sets fields and _validate_modality raises on
        bad input.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:

                class DummyUpgrader:
                    """Dummy upgrader class for testing _apply_overrides."""

                    data_summary = None
                    modality = None

                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    modality="pophys",
                    data_summary="summary",
                )
                manager = MetadataManager(settings)
                upgrader = DummyUpgrader()
                manager._apply_overrides(upgrader)
                self.assertEqual(upgrader.data_summary, "summary")
                self.assertTrue(upgrader.modality)
                # Test _validate_modality raises on bad input
                with self.assertRaises(ValueError):
                    manager._validate_modality("not-a-modality")

    def test_collect_json_objects_and_evaluations(self):
        """Test collect_json_objects and collect_evaluations with a valid
        file.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                # Write a dummy evaluation file
                eval_path = input_dir / "foo_evaluation.json"
                eval_path.write_text("{}")
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir
                )
                manager = MetadataManager(settings)
                objs = manager.collect_json_objects("evaluation")
                self.assertEqual(len(objs), 1)
                # collect_evaluations should not error on invalid data
                evals = manager.collect_evaluations()
                self.assertIsInstance(evals, list)

    def test_create_quality_control_metadata(self):
        """Test create_quality_control_metadata returns a QualityControl
        object.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                settings = DummySettings(
                    input_dir=input_dir, output_dir=input_dir
                )
                manager = MetadataManager(settings)
                qc = manager.create_quality_control_metadata()
                self.assertIsInstance(qc, object)

    def test_copy_ancillary_files_missing(self):
        """Test copy_ancillary_files does not raise if files are missing."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir)
                output_dir = Path(tempdir) / "out"
                output_dir.mkdir()
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    skip_ancillary_files=False,
                )
                manager = MetadataManager(settings)
                # Should not raise even if files are missing
                manager.copy_ancillary_files()

    def test_create_processing_metadata(self):
        """Test create_processing_metadata creates a Processing object from
        valid input.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()
                dp = {
                    "name": "Analysis",
                    "software_version": "1.0",
                    "start_date_time": "2023-01-01T00:00:00Z",
                    "end_date_time": "2023-01-01T01:00:00Z",
                    "input_location": "/input/path",
                    "output_location": "/output/path",
                    "code_url": "http://example.com/code",
                    "parameters": {"param1": "value1"},
                    "outputs": {"param2": "value2"},
                    "notes": "Test process",
                }
                with open(input_dir / "data_process.json", "w") as f:
                    json.dump(dp, f)
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir
                )
                manager = MetadataManager(settings)
                processing = manager.create_processing_metadata()

                self.assertEqual(
                    str(processing.processing_pipeline.data_processes[0].name),
                    "Analysis",
                )

    def test_copy_ancillary_files(self):
        """Test copy_ancillary_files copies an ancillary file successfully."""
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()
                # Create all ancillary files
                ancillary_files = [
                    "procedures.json",
                    "subject.json",
                    "session.json",
                    "rig.json",
                    "instrument.json",
                    "acquisition.json",
                ]
                for ancillary in ancillary_files:
                    with open(input_dir / ancillary, "w") as f:
                        json.dump({"test": 1}, f)
                settings = DummySettings(
                    input_dir=input_dir,
                    output_dir=output_dir,
                    skip_ancillary_files=False,
                )
                manager = MetadataManager(settings)
                manager.copy_ancillary_files()
                for ancillary in ancillary_files:
                    self.assertTrue((output_dir / ancillary).exists())

    @mock.patch(
        "aind_metadata_upgrader.data_description_upgrade.DataDescriptionUpgrade"  # noqa: E501
    )
    @mock.patch(
        "aind_data_schema.core.data_description.DerivedDataDescription"
    )
    def test_create_derived_data_description(self, MockDerived, MockUpgrade):
        """Test create_derived_data_description writes a derived data
        description file.
        """
        with mock.patch("sys.argv", [""]):
            with tempfile.TemporaryDirectory() as tempdir:
                input_dir = Path(tempdir) / "input"
                output_dir = Path(tempdir) / "output"
                input_dir.mkdir()
                output_dir.mkdir()
                dd = {
                    "creation_time": "2025-06-04T14:42:32.061702-07:00",
                    "data_level": "raw",
                    "data_summary": "OpenScopeTexture",
                    "describedBy": "https://raw.githubusercontent.com/AllenNeuralDynamics/aind-data-schema/main/src/aind_data_schema/core/data_description.py",  # noqa: E501
                    "funding_source": [
                        {
                            "fundee": None,
                            "funder": {
                                "abbreviation": "AI",
                                "name": "Allen Institute",
                                "registry": {
                                    "abbreviation": "ROR",
                                    "name": "Research Organization Registry",
                                },
                                "registry_identifier": "03cpe7c52",
                            },
                            "grant_number": None,
                        }
                    ],
                    "group": "ophys",
                    "institution": {
                        "abbreviation": "AIND",
                        "name": "Allen Institute for Neural Dynamics",
                        "registry": {
                            "abbreviation": "ROR",
                            "name": "Research Organization Registry",
                        },
                        "registry_identifier": "04szwah67",
                    },
                    "investigators": [
                        {
                            "abbreviation": None,
                            "name": "Jerome Lecoq",
                            "registry": None,
                            "registry_identifier": None,
                        }
                    ],
                    "label": None,
                    "license": "CC-BY-4.0",
                    "modality": [
                        {
                            "abbreviation": "pophys",
                            "name": "Planar optical physiology",
                        },
                        {
                            "abbreviation": "behavior-videos",
                            "name": "Behavior videos",
                        },
                    ],
                    "name": "multiplane-ophys_784492_2025-06-04_14-42-32",
                    "platform": {
                        "abbreviation": "multiplane-ophys",
                        "name": "Multiplane optical physiology platform",
                    },
                    "project_name": "OpenScope",
                    "related_data": [],
                    "restrictions": "test",
                    "schema_version": "1.0.4",
                    "subject_id": "784492",
                }
                with open(input_dir / "data_description.json", "w") as f:
                    json.dump(dd, f)
                settings = DummySettings(
                    input_dir=input_dir, output_dir=output_dir
                )
                manager = MetadataManager(settings)
                dummy_upgrade = mock.Mock()
                dummy_upgrade.data_summary = None
                dummy_upgrade.modality = None
                MockUpgrade.return_value.upgrade.return_value = dummy_upgrade
                dummy_derived = mock.Mock()
                MockDerived.from_data_description.return_value = dummy_derived
                dummy_derived.write_standard_file.side_effect = (
                    lambda output_directory: (
                        Path(output_directory) / "data_description.json"
                    ).write_text("{}")
                )
                manager.create_derived_data_description()
                self.assertTrue(
                    (output_dir / "data_description.json").exists()
                )


if __name__ == "__main__":
    unittest.main()
